#! /bin/bash

# /usr/sbin/sshd -D -e
service ssh start

# wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
# mv spark-sql-kafka-0-10_2.12-3.5.0.jar /usr/local/spark/spark-sql-kafka-0-10_2.12-3.5.0.jar

wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar
mv slf4j-api-1.7.30.jar /usr/local/spark/jars/slf4j-api-1.7.30.jar

sleep 40
/root/start-hadoop.sh
sleep 40

/root/start-kafka-zookeeper.sh
sleep 10




echo "DONE"
tail -f /dev/null
