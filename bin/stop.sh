#!/bin/bash
pwd_bak=$(pwd)
# 获取脚本所在目录
bin_dir=$(dirname $(readlink -f "$0"))
echo "bin_dir : ${bin_dir}"
# 项目基本目录
base_dir=$(dirname "${bin_dir}")
echo "base_dir : ${base_dir}"
# 项目名，建议配置为 maven 的默认jar命名风格
project_name=$(basename ${base_dir})
echo "project_name : ${project_name}"
# 检查程序是否已启动，如果已启动，则结束它
check_pid=$(ps -ef | grep java | grep ${project_name} | grep -v grep | awk '{print $2}')
if [ x"check_pid" != x"" ]; then
  echo "${project_name} is running. try kill it"
  kill -15 ${check_pid}
  sleep 10
  kill -9 ${check_pid}
fi
