#! /bin/bash

kafka-topics.sh --bootstrap-server localhost:9092 --topic job_data_topic --create --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --topic locations_topic --create --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --topic titles_topic --create --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --topic industries_topic --create --partitions 3 --replication-factor 1