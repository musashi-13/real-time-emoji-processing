# Concurrent Emoji Broadcast Over Event Driven Architecture

A scalable real-time system that captures and processes billions of user-generated emojis during live sporting events, built with Kafka for data streaming and Spark for real-time processing to ensure high concurrency and low latency.

## Team Members
* PES1UG22CS189 - Dinesh Kumar L C
* PES1UG22CS253 - Jayanth Ramesh
* PES1UG22CS275 - Karan Prasad Hathwar
* PES1UG22CS279 - Kaushik Bhat


## How to run:

* `./start.sh`

_starts kafka and zookeeper. change the directory variable_
* `./topic.sh`

_creates or replace all the needed topics_

* `python3 app.py`

_listens to /send-emoji_
* `spark-submit spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 main_consumer.py`

_processes data. resets every batch. batch = 2 seconds. aggregates every 1000 emojis. sends emojis to main-pub-topic._

* `python3 cluster_pub.py`

_reads from main-pub-topic. there are 3 clusters each with its own topic as clusterx-pub-topic. each cluster has 2 subscribers that read from the respective topic_
* `CLUSTER_TOPIC=cluster1-pub-topic python3 subscriber1.py` and `CLUSTER_TOPIC=cluster1-pub-topic python3 subscriber2.py`

_reads from the cluster topic specified in command and gets ports assigned based on it. each subscriber has 3 functions, notify-client to send emoji, register to register the client and deregister to do the opposite._

* `PORT=8001 subscriber=1 client.py`

_sends emojis to app.py that is listening to /send-emoji and receives emojis from the subscriber it is registered to. registration happens via polling since each subscriber can have only 2 clients. prints recieved emojis as a stream_
