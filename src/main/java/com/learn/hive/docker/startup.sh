#!/bin/bash

hadoop fs -mkdir       /tmp
hadoop fs -mkdir -p    /user/com.learn.hive/warehouse
hadoop fs -chmod g+w   /tmp
hadoop fs -chmod g+w   /user/com.learn.hive/warehouse

cd $HIVE_HOME/bin
./hiveserver2 --hiveconf com.learn.hive.server2.enable.doAs=false
