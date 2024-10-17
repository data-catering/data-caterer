#!/usr/bin/env bash

topic=${KAFKA_TOPIC:-account-topic}
local_folder=${LOCAL_MESSAGE_FOLDER:-app/src/test/resources/sample/kafka/messages}

echo "Getting data from kafka topic: ${topic}"
docker exec kafka bash -c "kafka-console-consumer --bootstrap-server kafka:29092 --topic ${topic} --from-beginning --max-messages 10 > /tmp/${topic}_messages.json"
echo "Moving data to local folder: ${local_folder}"
mkdir -p "${local_folder}"
docker cp "kafka:/tmp/${topic}_messages.json" "${local_folder}/${topic}_messages.json"
echo "Finished!"
