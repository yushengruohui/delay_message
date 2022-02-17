#!/bin/bash
echo "start.sh begin running"
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
# 检查程序是否已启动，如果已启动，则结束它
sh -c ${bin_dir}/stop.sh
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
#nohup java ${jvm_config} -jar ${base_dir}/lib/${tar_name}.jar >/dev/null 2>&1 &
java ${jvm_config} -jar ${base_dir}/lib/${tar_name}.jar
echo "${project_name} has started ,but may be exist exception.please check log"
