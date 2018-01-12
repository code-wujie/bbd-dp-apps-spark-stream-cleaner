package com.bbd.dataplatform.apps.spark.stream.cleaner.core;

import com.bbd.dataplatform.apps.common.config.Context;
import com.bbd.dataplatform.apps.common.constants.Constants;
import com.bbd.dataplatform.apps.common.constants.Constants.BBDKEY;
import com.bbd.dataplatform.apps.common.constants.Constants.NAMESPACE;
import com.bbd.dataplatform.apps.common.dubbo.DubboReferenceUtil;
import com.bbd.dataplatform.apps.common.json.JsonUtil;
import com.bbd.dataplatform.apps.common.kafka.KafkaProducerHandler;
import com.bbd.dataplatform.apps.common.log.model.LogModel;
import com.bbd.dataplatform.apps.common.model.CheckParamEntity;
import com.bbd.dataplatform.apps.common.utils.BbdInnerFlagUtil;
import com.bbd.dataplatform.apps.spark.stream.BasicStreamingProcess;
import com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils.FlumeLogUtil;
import com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils.PendingDataUtil;
import com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils.SeedLogUtil;
import com.bbd.dataplatform.provider.cleaner.facade.CleanerServiceFacade;
import com.bbd.dataplatform.provider.common.facade.mode.FacadeResponse;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: Rand
 * @Desciption:
 * @Date: Created in 17:47 2017/12/26
 * @Modified By:
 */
public class CleanerStreamingProcess extends BasicStreamingProcess {

	private static final long serialVersionUID = -3210520087731479662L;
	
	private CleanerServiceFacade facade;
    private KafkaProducerHandler kafkaProducerHandler;
    //清洗快速通道队列
    private static final String BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC = "BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC";


    @Override
    public void addCheckParam(Context context, List<CheckParamEntity> checkParams) {
        super.addCheckParam(context, checkParams);
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_DUBBO_ADDRESS_HOSTS, true));
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_DUBBO_CONSUMER_NAME, true));
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_KAFKA_BROKER_LIST,   true));
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_KAFKA_QUEUE_SUCCESS, true));
        checkParams.add(new CheckParamEntity(BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC, true));
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_KAFKA_QUEUE_PENDING, true));
        checkParams.add(new CheckParamEntity(Constants.PARAM.BBD_DP_KAFKA_QUEUE_SEEDLOG, true));
    }


    @Override
    public void process(Context context, Iterator<Map<String, Object>> messages) {

        while (messages.hasNext()) {
            //读取数据
            Map<String, Object> message = messages.next();
            //业务处理
            try {
                long startTime = System.currentTimeMillis();
                //发送请求进行处理
                FacadeResponse response = this.facade.handle(message);
                long endTime = System.currentTimeMillis();
                long costTime = endTime - startTime;

                //数据处理
                if (response.getStatusCode().isok()) {
                    for (Map<String, Object> data : response.getDatas()) {
                        /**清洗处理： 快速通道处理时，如果为历史反填充数据则发送到普通通道处理,其他数据发送到快速通道,普通通道处理时，可以将两个通道配置为同一个队列 **/
                        if (BbdInnerFlagUtil.isBackFill(data)) {
                        	//当数据存在指纹信息的情况下，重新设置数据的指纹信息
                        	if(StringUtils.isNotBlank(MapUtils.getString(message, BBDKEY.BBD_DATA_UNIQUE_ID))){
                            	data.put(BBDKEY.BBD_DATA_UNIQUE_ID, DigestUtils.md5Hex(UUID.randomUUID().toString()));
                        	}
                            //历史反填充数据,发送到普通通道处理
                            this.kafkaProducerHandler.push(context.getString(Constants.PARAM.BBD_DP_KAFKA_QUEUE_SUCCESS), JsonUtil.toString(data));
                        } else {
                            this.kafkaProducerHandler.push(context.getString(BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC), JsonUtil.toString(data));
                        }
                        printFlumeLog(context, FlumeLogUtil.getSuccessLog(message, data, costTime));
                    }
                } else {
                    this.kafkaProducerHandler.push(context.getString(Constants.PARAM.BBD_DP_KAFKA_QUEUE_PENDING), PendingDataUtil.get(response));
                    printFlumeLog(context, FlumeLogUtil.getErrorLog(response, costTime));
                }

                //发送种子日志
                String seedLog = SeedLogUtil.getSeedLog(response);
                this.kafkaProducerHandler.push(context.getString(Constants.PARAM.BBD_DP_KAFKA_QUEUE_SEEDLOG), seedLog);

            } catch (Exception e) {
                //数据处理异常，直接放到待定库
                logger.error("spark stream cleaner 出现严重的异常：", e);
                printFlumeLog(context, FlumeLogUtil.getExceptionLog(message, e));
            }
        }
        //刷新kafka的存储数据
        this.kafkaProducerHandler.flush();
    }

    @Override
    public <T> void printFlumeLog(Context context, LogModel<T> log) {
        if (log != null) {
            Logger LOGGER = LogManager.getLogger(NAMESPACE.FLUME_LOG_NAMESPACE + this.getClass().getSimpleName());
            LOGGER.info(JsonUtil.toString(log));
        }
    }

    @Override
    public void dInit(Context context) {

        String brokers = context.getString(Constants.PARAM.BBD_DP_KAFKA_BROKER_LIST);
        this.kafkaProducerHandler = KafkaProducerHandler.getInstance(brokers);

        String consumer = context.getString(Constants.PARAM.BBD_DP_DUBBO_CONSUMER_NAME);
        String address = context.getString(Constants.PARAM.BBD_DP_DUBBO_ADDRESS_HOSTS);
        this.facade = DubboReferenceUtil.getReference(address, consumer, CleanerServiceFacade.class);

    }
}
