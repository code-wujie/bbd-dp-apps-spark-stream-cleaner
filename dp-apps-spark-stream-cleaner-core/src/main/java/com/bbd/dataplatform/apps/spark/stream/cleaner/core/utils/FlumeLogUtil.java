package com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils;

import com.bbd.dataplatform.apps.common.constants.Constants.BBDKEY;
import com.bbd.dataplatform.apps.common.constants.StreamingProcessStage.PROCESS_STAGE;
import com.bbd.dataplatform.apps.common.constants.StreamingProcessStage.ProcessStageCode;
import com.bbd.dataplatform.apps.common.log.model.LogModel;
import com.bbd.dataplatform.provider.common.facade.mode.FacadeResponse;

import java.util.Map;

public class FlumeLogUtil {
	
	/**
	 * 获取数据中的指纹信息ID
	 * @param data
	 * @return
	 */
	private static String getDataUniqueId(Map<String, Object> data){
		Object unique_id = data.get(BBDKEY.BBD_DATA_UNIQUE_ID);
		if(unique_id != null && unique_id.toString().length() > 0){
			return unique_id.toString();
		}
		else{
			return null;
		}
	}
	
	/**
	 * 成功日志
	 * @param response
	 * @param responseTime
	 * @return
	 */
	public static LogModel<String> getSuccessLog(FacadeResponse response, long responseTime){
		String uniqueId = getDataUniqueId(response.getData());
		if(uniqueId != null && uniqueId.length() > 0){
			LogModel<String> log = new LogModel<>(uniqueId, PROCESS_STAGE.CLEANER.toString());
			log.setResponseTime(responseTime);
			log.setErrorMessage("清洗处理成功");
			log.setStatusCode(ProcessStageCode.DP_CLEANER_PROCESS_SUC);
			return log;
		}
		return null;
	}
	
	/**
	 * 错误日志
	 * @param response
	 * @param responseTime
	 * @return
	 */
	public static LogModel<String> getErrorLog(FacadeResponse response, long responseTime) {
		String uniqueId = getDataUniqueId(response.getData());
		if (uniqueId != null && uniqueId.length() > 0) {
			LogModel<String> log = new LogModel<>(uniqueId, PROCESS_STAGE.CLEANER.toString());
			log.setResponseTime(responseTime);
			log.setErrorMessage("清洗处理成功");
			log.setStatusCode(ProcessStageCode.DP_CLEANER_PROCESS_ERO);
			return log;
		}
		return null;
	}
	
	/**
	 * 异常日志
	 * @param message
	 * @param exception
	 * @return
	 */
	public static LogModel<String> getExceptionLog(Map<String, Object> message, Exception exception){
		String uniqueId = getDataUniqueId(message);
		if(uniqueId != null && uniqueId.length() > 0){
			LogModel<String> log = new LogModel<>(uniqueId, PROCESS_STAGE.CLEANER.toString());
			log.setErrorMessage("清洗处理异常：" + exception.getMessage());
			log.setStatusCode(ProcessStageCode.DP_GLOBAL_EXCEPTION);
			return log;
		}
		return null;
	}
	
}
