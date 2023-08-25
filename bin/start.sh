#!/bin/env bash

HERE=$(cd $(dirname "$0"); pwd)
BASE=$(dirname "$HERE")
META_SERVER_PATH="$BASE/metaServer"
DATA_SERVER_PATH="$BASE/dataServer"
META_SERVER_PORT=(8000 8001 8002)
DATA_SERVER_PORT=(9000 9001 9002 9003)
META_SERVER_CONF=('' replica1 replica2)
ZOOKEEPER_URL=10.0.0.201:2181

cd $META_SERVER_PATH
JAR_FILE=$(ls *.jar)
for ((i=0;i<${#META_SERVER_PORT[@]};i++)); do
  port=${META_SERVER_PORT[$i]}
  conf=${META_SERVER_CONF[$i]}
  nohup java -Dloader.path=./lib -Dzookeeper.addr=${ZOOKEEPER_URL} -Dspring.profiles.active=${conf} -jar $JAR_FILE --server.port=${port} > ${BASE}/console.log 2>&1 &
  echo start metaServer port ${port}
  sleep 1
done

cd $DATA_SERVER_PATH
JAR_FILE=$(ls *.jar)
for port in ${DATA_SERVER_PORT[@]}; do
  nohup java -Dloader.path=./lib -Dzookeeper.addr=${ZOOKEEPER_URL} -jar $JAR_FILE --server.port=${port} > ${BASE}/console.log 2>&1 &
  echo start dataServer port ${port}
  sleep 1
done
