#!/bin/bash
#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

usage="Usage: status-history-server.sh status <command> "

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi
startStop=$1
shift
command=$1
SH_DIR=`dirname $0`


status(){
  echo "start check $command status"
        pid=`ps aux | grep -iw org.apache.spark.deploy.history.HistoryServer | grep -v grep | awk '{print $2}'`
        echo "pid is : $pid"
        kill -0 $pid
        if [ $? -eq 0 ]
        then
                echo "$command is  running "
        else
                echo "$command  is not running"
                exit 1
        fi
}
case $startStop in
  (status)
          status
        ;;
  (*)
    echo $usage
    exit 1
    ;;
esac


echo "End $startStop $command."