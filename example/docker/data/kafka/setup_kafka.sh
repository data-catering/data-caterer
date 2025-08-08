host=kafkaserver
# blocks until kafka is reachable
kafka-topics --bootstrap-server $host:29092 --list

echo 'Creating kafka topics'
kafka-topics --bootstrap-server $host:29092 --create --if-not-exists --topic account-topic --replication-factor 1 --partitions 1

echo 'Successfully created the following topics:'
kafka-topics --bootstrap-server $host:29092 --list

#kafka-topics --delete --topic account-topic --bootstrap-server localhost:9092
#kafka-topics --bootstrap-server localhost:9092 --create --topic customer-product-topic --replication-factor 1 --partitions 1
#kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-product-topic --from-beginning
#kafka-console-consumer --bootstrap-server localhost:9092 --topic customer-topic --from-beginning
#kafka-console-consumer --bootstrap-server localhost:9092 --topic account-topic --from-beginning