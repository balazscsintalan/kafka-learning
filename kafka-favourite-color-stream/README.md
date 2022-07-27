kafka-topics.sh --bootstrap-server 172.27.158.25:9092 --create --topic favourite-color-input --partitions 2

kafka-topics.sh --bootstrap-server 172.27.158.25:9092 --create --topic favourite-color-output --partitions 2 

kafka-topics.sh --bootstrap-server 172.27.158.25:9092 --create --topic favourite-color-compacted --partitions 2 --config cleanup.po
licy=compact

kafka-console-producer.sh --bootstrap-server 172.27.158.25:9092 --topic favourite-color-input

>stephane,blue
>john,green
>stephane,red
>alice,red


kafka-console-consumer.sh --bootstrap-server 172.27.158.25:9092 --from-beginning --topic favourite-color-output --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

kafka-console-consumer.sh --bootstrap-server 172.27.158.25:9092 --from-beginning --topic favourite-color-compacted --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

