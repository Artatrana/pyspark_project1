FROM openjdk:11

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install pyspark pyyaml

COPY . /app
WORKDIR /app
CMD ["python3", "src/main.py"]