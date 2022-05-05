#!/bin/sh
pid=`ps -ef | grep 'python3 -m sfme' | grep -v grep | awk '{print $2}'`
if [ $pid ]; then
  echo "get sfme pid: ${pid}"
  kill -9 ${pid}
  echo "kill sfme pid=${pid}"
fi
cd /sfme
nohup python3 -m sfme > /dev/null 2>&1 &
echo "sfme pid = `ps -ef | grep 'python3 -m sfme' | grep -v grep | awk '{print $2}'`"
