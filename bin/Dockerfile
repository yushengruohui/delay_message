FROM adoptopenjdk
VOLUME /usr/local/docker
MAINTAINER yusheng
ENV tar_name="kafka_delay_sqlite-20220201.tar.gz"
ENV base_dir="/usr/local/kafka_delay_sqlite"
RUN ["sh", "-c", "mkdir -p ${base_dir}"]
ADD ["/target/${tar_name}","${base_dir}"]
WORKDIR ${base_dir}
ENTRYPOINT ["sh", "-c","bin/start.sh"]
