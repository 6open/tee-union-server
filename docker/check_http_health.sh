#!/bin/bash

http_res=$(curl -s http://127.0.0.1:6700/healthz 2> /dev/null)

echo "$http_res"
# 初始化状态标记
http_status=0

# 检查HTTP响应
if [[ "$http_res" == "OK" ]]; then
    echo "HTTP is OK"
else
    echo "HTTP is failed"
    http_status=1
fi

# 判断总体状态
if [[ $http_status -eq 0 ]]; then
    exit 0
else
    exit 1
fi

