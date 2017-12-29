package com.bbd.dataplatform.apps.spark.stream.cleaner.assembly;

import com.bbd.dataplatform.apps.spark.stream.cleaner.core.CleanerStreamingProcess;

/**
 * @Author: Rand
 * @Desciption:
 * @Date: Created in 17:58 2017/12/28
 * @Modified By:
 */
public class Main {

    public static void main(String[] args) {
        //提交作业
        new CleanerStreamingProcess().run(args);
    }
}
