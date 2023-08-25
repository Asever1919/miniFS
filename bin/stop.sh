#!/bin/env bash

function shutdown_java() {
  PID=$(jps -l | grep "$1" | awk '{print $1}')
  for pid in ${PID}; do
    if [[ -n ${pid} ]]; then
        echo shutdown $1 pid ${pid}
        kill ${pid}
        sleep 1
    fi
  done
}

shutdown_java metaServer
shutdown_java dataServer
