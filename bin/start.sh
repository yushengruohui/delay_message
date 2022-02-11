#!/bin/bash
echo "start.sh running"
# 获取脚本所在目录
bin_dir=$(dirname "$(readlink -f "$0")")
echo "bin_dir : ${bin_dir}"
# 项目基本目录
base_dir=$(dirname "${bin_dir}")
echo "base_dir : ${base_dir}"
# 项目名，建议配置为 maven 的默认jar命名风格
tar_name=$(basename "${base_dir}")
project_name=${tar_name%-*}
echo "project_name : ${project_name}"
# jar包所在目录
lib_dir=${base_dir}/lib
echo "lib_dir : ${lib_dir}"
# 检查程序是否已启动，如果已启动，则结束它
check_pid=$(ps -ef | grep java | grep "${project_name}" | grep -v grep | awk '{print $2}')
if [ x"${check_pid}" != x"" ]; then
  echo "${project_name} is running.pid is ${check_pid}. try kill it"
  kill -15 "${check_pid}"
  sleep 10s
  kill -9 "${check_pid}"
fi
# 初始化基础环境
source /etc/profile
ulimit -n 640000
ulimit -u 640000
# 配置jvm的堆内存
total_men=$(head /proc/meminfo -n 1 | awk '{print $2}')
echo "total_men : ${total_men} kb"
jvm_men=$(("${total_men}" / 1024 / 4))
if [ ${jvm_men} -lt 1024 ]; then jvm_men=1024; fi
echo "jvm_men : ${jvm_men} mb"
jvm_config="\
-Xms${jvm_men}m \
-Xmx${jvm_men}m \
-Xss256k \
-server \
-Djava.security.edg=file:/dev/./urandom \
-Dfile.encoding=UTF-8 \
-XX:+HeapDumpOnOutOfMemoryError \
-XX:HeapDumpPath=${base_dir}/ \
"
cd ${base_dir} || (echo "lib_dir not exist" && exit 1)
#nohup java ${jvm_config} -jar ${lib_dir}/${tar_name}.jar >/dev/null 2>&1 &
java ${jvm_config} -jar ${lib_dir}/${tar_name}.jar
echo "${project_name} has started ,but may be exist exception.please check log"
