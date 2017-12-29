package com.bbd.dataplatform.apps.spark.stream.cleaner.core.utils;

import com.bbd.dataplatform.apps.common.constants.Constants.BBDKEY;
import com.bbd.dataplatform.apps.common.seedlog.BbdSeedLogApi;
import com.bbd.dataplatform.apps.common.seedlog.SeedState;
import com.bbd.dataplatform.apps.common.utils.MapUtil;
import com.bbd.dataplatform.provider.common.facade.mode.FacadeResponse;

import java.util.Map;

public class SeedLogUtil {
	
	private static final String BBD_TABLE_QYXX = "qyxx";
	
	/**
	 * 判断是否需要进行种子日志处理
	 * @param response	
	 * @return
	 */
	private static boolean hasSeed(FacadeResponse response){
		Map<String, Object> data = response.getData();
		if(BBD_TABLE_QYXX.equalsIgnoreCase(MapUtil.getValue(data, BBDKEY.BBD_TABLE, "").toString()) && data.containsKey(BBDKEY.BBD_SEED)){
			String seed = MapUtil.getValue(data, BBDKEY.BBD_SEED, "").toString();
			if(seed != null && seed.length() > 8){
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 种子日志处理
	 * @param response	消息
	 * @return
	 */
	public static String getSeedLog(FacadeResponse response){
		if(hasSeed(response)){
			if(response.getStatusCode().isok()){
				return BbdSeedLogApi.getLogs(SeedState.BBD_SEED_IS_CLEAN_SUC, response.getData());
			}
			else{
				return BbdSeedLogApi.getLogs(SeedState.BBD_SEED_IS_CLEAN_ERO, response.getData());
			}
		}
		return null;
	}
}	
