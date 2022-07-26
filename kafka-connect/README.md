bootstrap-servers in:
- server.properties
- connect-standalone.properties


Kafka Connect:  
https://kafka.apache.org/quickstart

> echo -e "foo\nbar" > test.txt

Run connect with for example (test file source and sink):
> bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties

List available connectors:  
`curl localhost:8083/connector-plugins | jq`

List running connectors  
`curl localhost:8083/connectors | jq`  
