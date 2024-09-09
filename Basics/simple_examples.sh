/usr/local/lib/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --create --topic orders --partitions 3 --replication-factor 3
/usr/local/lib/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic orders
/usr/local/lib/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic orders --from-beginning