`kafka-topics.sh --bootstrap-server 172.27.149.48:9092 --create --topic word-count-input --partitions 2`  

`kafka-topics.sh --bootstrap-server 172.27.149.48:9092 --create --topic word-count-output --partitions 2`  

`kafka-console-consumer.sh --bootstrap-server 172.27.149.48:9092 --from-beginning --topic word-count-output --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer`