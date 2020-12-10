# Content
A high performance flink project reads from kafka and writes to HDFS

# Prerequisite
* Java  jdk11
* Flink 1.11.2

## Usage
```
git clone https://github.com/Sunny-Island/Kafka2hdfs.git
cd Kafka2hdfs
mvn package
./bin/flink run -m yarn-cluster -yn 1 -yjm 1024 -ytm 1024 Kafka2hdfs.jar
```


