#!/bin/bash -x
export APP_NAME=hackathon-knight-1.0.0.jar

export EXECUTOR_MEMORY=1G
export TOTAL_EXECUTOR_CORES=1

#eg:spark://<masteripaddress>:6066, This can be obtained from cluster UI
#This setting is not needed for client mode deployment
export SPARK_MASTER_URL=
export CLASS_NAME="com.knight.streaming.StreamNotifier"
export DEPLOY_MODE=


