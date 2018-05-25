package com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils;

import com.bbd.dataplatform.apps.common.constants.Constants.BBDKEY;
import com.bbd.dataplatform.apps.common.constants.StreamingProcessStage.PROCESS_STAGE;
import com.bbd.dataplatform.provider.common.facade.mode.FacadeResponse;

import java.util.Map;

public class PendingDataUtil {
	
	public static Map<String, Object> get(FacadeResponse response){
		Map<String, Object> data = response.getData();
		data.put(BBDKEY.BBD_ERROR_PROCESS, PROCESS_STAGE.CLEANER);
		data.put(BBDKEY.BBD_ERROR_LOG, response.getErrors());
		return data;
	}

	public static Map<String, Object> get(Map<String, Object> data, Exception exception) {
		data.put(BBDKEY.BBD_ERROR_PROCESS, PROCESS_STAGE.CLEANER_VALIDATE);
		data.put(BBDKEY.BBD_ERROR_LOG, exception.getMessage());
		return data;
	}
}
