package com.bbd.dataplatform.apps.spark.stream.cleaner.test;

import com.bbd.dataplatform.apps.spark.stream.cleaner.core.CleanerStreamingProcess;

/**
 * @Author: Rand
 * @Desciption:
 * @Date: Created in 19:35 2017/12/27
 * @Modified By:
 */
public class CleanerStreamingProcessTest {


    public static  void main(String[] args){

        System.setProperty("env","dev");
        System.setProperty("BBD_DP_IS_DEBUG", "true");
        new CleanerStreamingProcess().run(args);
    }
}
