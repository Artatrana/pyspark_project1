FROM openjdk:11

# Install dependencies
RUN apt-get update && apt-get install -y curl python3 python3-pip && \
    apt-get clean

# Set environment variables
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3

# Install Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    ln -s /opt/spark/bin/spark-submit /usr/bin/spark-submit

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Copy project files
WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Default command
CMD ["spark-submit", "src/main.py"]
