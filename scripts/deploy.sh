#!/bin/bash -x

source settings.sh

BASE_DIR=$(cd `dirname $0`/..; pwd)
pushd $BASE_DIR/bin

JARS_HOME=$BASE_DIR/lib
APP_JAR=$JARS_HOME/$APP_NAME

JARS="$JARS_HOME/nscala-time_2.10-2.0.0.jar,\
$JARS_HOME/kafka_2.10-0.8.2.1.jar,\
$JARS_HOME/kafka-clients-0.8.2.1.jar,\
$JARS_HOME/spark-streaming-kafka_2.10-1.6.2.jar,\
$JARS_HOME/zkclient-0.3.jar,\
$JARS_HOME/metrics-core-2.2.0.jar,\
$JARS_HOME/zookeeper-3.4.6.jar"

JARS_COLON_SEP=`echo $JARS | sed 's/,/:/g'`

INPUT_PATH=`readlink -f $1`

SPARK_SUBMIT_CMD="/home/vdinakaran/Downloads/spark-1.6.2-bin-hadoop2.6/bin/spark-submit"

if [ "$DEPLOY_MODE" == "cluster" ]
then
 $SPARK_SUBMIT_CMD --jars $JARS \
  --executor-memory $EXECUTOR_MEMORY \
  --total-executor-cores $TOTAL_EXECUTOR_CORES \
  --deploy-mode "cluster" \
  --supervise \
  --driver-class-path $JARS_COLON_SEP \
  --class $CLASS_NAME $APP_JAR  $INPUT_PATH --files $INPUT_PATH
else
export SPARK_LOCAL_IP=`ifconfig | grep eth0 -1 | grep -i inet | awk '{ print $2 }' | cut -d':' -f2`
 $SPARK_SUBMIT_CMD -v  --jars $JARS \
 --executor-memory $EXECUTOR_MEMORY \
 --total-executor-cores $TOTAL_EXECUTOR_CORES \
 --driver-class-path $JARS_COLON_SEP \
 --deploy-mode "client" \
 --class $CLASS_NAME $APP_JAR  $INPUT_PATH --files $INPUT_PATH
fi
