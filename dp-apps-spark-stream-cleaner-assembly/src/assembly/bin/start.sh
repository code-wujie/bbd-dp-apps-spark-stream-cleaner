#!/bin/bash
basepath=$(cd `dirname $0`; pwd);cd $basepath; cd ../;basepath=`pwd`;

#运行参数
start_model=yarn-cluster
driver_memory=2G
driver_cores=1
num_executors=5
executor_cores=2
executor_memory=2G 

#获取参数
mainclassjar=`find "${basepath}/libs" -name 'dp-apps-spark-stream-*-assembly-*.jar'|xargs echo | tr ' ' ','`
dependencyjars=`find "${basepath}/libs" -name '*.jar'|grep -v ${mainclassjar} |xargs echo | tr ' ' ','`
modulename=`echo ${mainclassjar} | sed 's/.*spark-stream-\(.*\)-assembly.*/\1/g' | sed 's/-/./g'`
mainclass="com.bbd.dataplatform.apps.spark.stream.${modulename}.assembly.Main"
configfile="${basepath}/conf/log4j2.xml,${basepath}/conf/log4j.properties"
runparams="--master ${start_model} --driver-memory ${driver_memory} --driver-cores ${driver_cores} --num-executors ${num_executors} --executor-cores ${executor_cores} --executor-memory ${executor_memory}"

#执行任务
spark-submit ${runparams} --files ${configfile} --jars ${dependencyjars} --class ${mainclass} ${mainclassjar} $@

