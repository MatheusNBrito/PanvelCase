# Use the Apache Airflow base image
FROM apache/airflow:2.10.2

# Instala dependências do sistema e ferramentas
USER root

# Instala Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean;

# Configura variáveis de ambiente para o Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Instala Hadoop
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://downloads.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz && \
    tar -xzf hadoop-3.4.0.tar.gz -C /usr/local/ && \
    mv /usr/local/hadoop-3.4.0 /usr/local/hadoop && \
    rm hadoop-3.4.0.tar.gz;

# Configura variáveis de ambiente para o Hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH

# Instala Spark
RUN wget https://downloads.apache.org/spark/spark-3.4.4/spark-3.4.4-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.4-bin-hadoop3.tgz -C /usr/local/ && \
    mv /usr/local/spark-3.4.4-bin-hadoop3 /usr/local/spark && \
    rm spark-3.4.4-bin-hadoop3.tgz;

# Configura variáveis de ambiente para o Spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Instala Python e bibliotecas necessárias
RUN apt-get install -y python3 python3-pip && \
    pip3 install pyspark;

# Retorna para o usuário airflow
USER airflow

# Adiciona os scripts de inicialização e outros códigos necessários (se houver)
COPY ./dags /opt/airflow/dags

# Comando padrão para iniciar o Airflow
CMD ["airflow", "scheduler"]
