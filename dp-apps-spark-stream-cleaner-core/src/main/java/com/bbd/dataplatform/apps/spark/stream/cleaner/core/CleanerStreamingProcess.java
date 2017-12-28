package com.bbd.dataplatform.apps.spark.stream.cleaner.core;

import com.bbd.dataplatform.apps.common.config.Context;
import com.bbd.dataplatform.apps.common.constants.Constants;
import com.bbd.dataplatform.apps.common.constants.StreamingProcessStage;
import com.bbd.dataplatform.apps.common.dubbo.DubboReferenceUtil;
import com.bbd.dataplatform.apps.common.json.JsonUtil;
import com.bbd.dataplatform.apps.common.kafka.KafkaProducerHandler;
import com.bbd.dataplatform.apps.common.log.model.LogModel;
import com.bbd.dataplatform.apps.common.seedlog.BbdSeedLogApi;
import com.bbd.dataplatform.apps.common.seedlog.SeedState;
import com.bbd.dataplatform.apps.common.utils.BbdInnerFlagUtil;
import com.bbd.dataplatform.apps.common.utils.MapUtil;
import com.bbd.dataplatform.apps.spark.stream.BasicStreamingProcess;
import com.bbd.dataplatform.provider.cleaner.facade.CleanerServiceFacade;
import com.bbd.dataplatform.provider.common.facade.mode.FacadeResponse;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: Rand
 * @Desciption:
 * @Date: Created in 17:47 2017/12/26
 * @Modified By:
 */
public class CleanerStreamingProcess extends BasicStreamingProcess {

    private static final Context context = Context.getInstance();
    private CleanerServiceFacade facade;
    private KafkaProducerHandler kafkaProducerHandler;
    private  String processingSeedLogQueue = context.getString("BBD_DP_KAFKA_SEED_LOG_TOPIC", "");
    private  String processingSuccessQueue = context.getString("BBD_DP_KAFKA_CLEANER_SLOW_SUCCESS_TOPIC", "");
    private  String processingSuccessFastQueue = context.getString("BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC", "");
    private  String processingFailQueue = context.getString("BBD_DP_KAFKA_SEED_LOG_TOPIC", "");

    @Override
    public void process(Context context, Iterator<Map<String, Object>> messages) {

        List<ProducerRecord<String, String>> producerRecordList = new ArrayList<>();
        while (messages.hasNext()) {

            Long start = System.currentTimeMillis();
            Map<String, Object> message = messages.next();
            String uniqueId = MapUtil.getValue(message, Constants.BBDKEY.BBD_DATA_UNIQUE_ID, "").toString();
            LogModel logModel = new LogModel(uniqueId, StreamingProcessStage.PROCESS_STAGE.CLEANER.toString());
            try {
                FacadeResponse response = facade.handle(message);
                Long time = System.currentTimeMillis() - start;

                //种子状态初始化
                int bbdSeedState = 0;

                logModel.setResponseTime(time);
                if (response.getStatusCode().isok()) {
                    //设置种子状态
                    bbdSeedState = SeedState.BBD_SEED_IS_CLEAN_SUC;
                    for (Map<String, Object> data : response.getDatas()) {

                        //发送数据至Kafka
                        if (!data.containsKey(Constants.BBDKEY.BBD_ERROR_LOG)) {
                            /**清洗处理：
                             * 快速通道处理时，如果为历史反填充数据则发送到普通通道处理,其他数据发送到快速通道
                             *普通通道处理时，可以将两个通道配置为同一个队列
                             * */
                            if (BbdInnerFlagUtil.isBackFill(data)) {
                                //历史反填充数据,发送到普通通道处理
                                logger.debug("processingSlowSuccessQueue {}", processingSuccessQueue);
                                kafkaProducerHandler.push(processingSuccessQueue, JsonUtil.toString(data));
                            } else {
                                logger.debug("processingFastSuccessQueue {}", processingSuccessFastQueue);
                                kafkaProducerHandler.push(processingSuccessFastQueue, JsonUtil.toString(data));
                            }
                        }
                    }

                    logModel.setStatusCode(StreamingProcessStage.ProcessStageCode.DP_CLEANER_PROCESS_SUC);
                    logModel.setErrorMessage("清洗处理成功");
                } else {
                    //设置种子状态
                    bbdSeedState = SeedState.BBD_SEED_IS_CLEAN_ERO;

                    message.put(Constants.BBDKEY.BBD_ERROR_PROCESS, StreamingProcessStage.PROCESS_STAGE.CLEANER.toString());
                    message.put(Constants.BBDKEY.BBD_ERROR_LOG, response.getErrors());
                    kafkaProducerHandler.push(processingFailQueue, JsonUtil.toString(message));

                    logModel.setStatusCode(StreamingProcessStage.ProcessStageCode.DP_CLEANER_PROCESS_ERO);
                    logModel.setErrorMessage(response.getErrors());
                }

                //处理种子日志
                if (bbdSeedState != 0) {
                    String log = BbdSeedLogApi.getLogs(bbdSeedState, message);
                    if (log != null) {
                        //数据不是历史数据，更改种子状态
                        kafkaProducerHandler.push(processingSeedLogQueue, log);
                        logger.debug("seed change state for : " + bbdSeedState);
                    }
                }
            } catch (Exception e) {
                logger.error("清洗处理失败!", e);
                logModel.setErrorMessage(e.getMessage());
                logModel.setStatusCode("");
                printFlumeLog(context, logModel);
            }
        }
        kafkaProducerHandler.flush();
    }

    @Override
    public <T> void printFlumeLog(Context context, LogModel<T> log) {
        Logger LOGGER = LogManager.getLogger("com.bbd.flume.cleaner." + CleanerStreamingProcess.class.getSimpleName());
        LOGGER.info(JsonUtil.toString(log));

    }

    @Override
    public void dInit(Context context) {

        processingSeedLogQueue = context.getString("BBD_DP_KAFKA_SEED_LOG_TOPIC", "");
        processingSuccessQueue = context.getString("BBD_DP_KAFKA_CLEANER_SLOW_SUCCESS_TOPIC", "");
        processingSuccessFastQueue = context.getString("BBD_DP_KAFKA_CLEANER_FAST_SUCCESS_TOPIC", "");
        processingFailQueue = context.getString("BBD_DP_KAFKA_SEED_LOG_TOPIC", "");

        String brokerList  = context.getString("BBD_DP_KAFKA_BROKER_LIST","");
        this.kafkaProducerHandler = KafkaProducerHandler.getInstance(brokerList);

        String zkAddress = context.getString("BBD_DP_DUBBO_PROVIDER_ZK", "zookeeper://10.28.102.151:2181?backup=10.28.102.152:2181,10.28.102.153:2181");
        this.facade = DubboReferenceUtil.getReference(zkAddress, "bbd-dp-cleaner", CleanerServiceFacade.class);

    }
}
