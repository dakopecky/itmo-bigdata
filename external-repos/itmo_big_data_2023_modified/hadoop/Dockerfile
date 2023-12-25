FROM ubuntu:latest AS build_base

RUN apt-get update -y \
    && apt-get install -y openjdk-8-jdk \
    && apt-get install -y lsb-release \
    && apt-get install -y net-tools \
    && apt-get install -y curl

RUN DEBIAN_FRONTEND="noninteractive" \
    TZ="Europe/Moscow" \
    apt-get install -y tzdata

RUN ln -sf /usr/share/zoneinfo/Europe/Moscow /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata


FROM build_base AS build_hadoop_base

WORKDIR /opt

RUN curl -o hadoop-3.3.6.tar.gz https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xzf hadoop-3.3.6.tar.gz -C /opt
RUN rm hadoop-3.3.6.tar.gz
RUN ln -sf /opt/hadoop-3.3.6 /opt/hadoop

RUN apt-get update -y \
    && apt-get install -y python3-pip

RUN mkdir -p /tmp/hadoop-hadoop

WORKDIR /opt/hadoop

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# fix WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop


FROM build_hadoop_base AS build_hadoop

FROM build_hadoop_base AS hive_hadoop_base
ENV HIVE_HOME /opt/hive
ENV PATH $HIVE_HOME/bin:$PATH

WORKDIR /opt

RUN mkdir /home/hive
RUN curl -o apache-hive-3.1.3-bin.tar.gz https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
RUN tar -xzf apache-hive-3.1.3-bin.tar.gz -C /opt && mv apache-hive-3.1.3-bin hive && rm apache-hive-3.1.3-bin.tar.gz

FROM hive_hadoop_base AS hive_hadoop


FROM hive_hadoop_base AS jupyter_hadoop

WORKDIR /opt

RUN pip3 install notebook

FROM hive_hadoop_base AS zeppelin_hadoop

WORKDIR /opt

RUN curl -o zeppelin-0.10.1-bin-all.tgz https://dlcdn.apache.org/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz
RUN tar -xzf zeppelin-0.10.1-bin-all.tgz -C /opt && mv zeppelin-0.10.1-bin-all zeppelin && rm zeppelin-0.10.1-bin-all.tgz

COPY zeppelin_conf/zeppelin-site.xml /opt/zeppelin/conf/




