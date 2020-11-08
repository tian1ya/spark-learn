#!/bin/zsh

mvn clean package -Dmaven.test.skip=true
scp /Users/xuxliu/Ifoods/scala/spark/myDevEnv/target/learning-1.0-SNAPSHOT.jar root@192.168.99.100:/opt/bigData/spark/myJars

# learning-1.0-SNAPSHOT.jar

#./bin/spark-submit --master spark://spark-master:7077  --class com.bigData.spark.core.wordCount /opt/bigData/spark/myJars/learning-1.0-SNAPSHOT.jar

