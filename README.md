## 模块名称
流式处理-清洗

## 依赖服务
* 清洗 http://git.bbdops.com/bbd-dp/bbd-dp-provider-cleaner.git

## 模块功能
* 调用清洗服务，对数据做清洗处理。
* 企业信息数据可能返回包括历史反填充数据，即一条数据会变成多条。

## 输入输出
* 输入：统一字段处理成功的数据
* 输出：
    * 清洗成功数据。
    * 清洗失败数据，写入待定队列。
    * 种子日志。
        
## 部署

#### 获取项目
    git clone http://git.bbdops.com/bbd-dp/bbd-dp-apps-spark-stream-cleaner.git
    
#### 编译
    cd bbd-dp-apps-spark-stream-cleaner  
    PRO:mvn clean package -Ppro -Dskiptests  
    TEST:mvn clean package -Ptest -Dskiptests  
    编译的目标文件将会在dp-apps-spark-stream-cleaner-assembly/target下生成为dp-apps-spark-stream-cleaner-assembly-1.0.0-assembly.zip

#### 发布
    将编译得到的zip文件移动到相应的项目目录下。
    
#### 启动
    unzip dp-apps-spark-stream-cleaner-assembly-1.0.0-assembly.zip
    cd  dp-apps-spark-stream-cleaner-assembly-1.0.0
    sh bin/start.sh