# Overview
Simple test application that connects to Kraken's publically available websocket to collect cryptocurrency trading information. The application sets up a Kafka instance with two replicas and three partitions for up to three topics to be monitored and to have a back-up replica incase one of them fails. The two brokers interact through their controllers which are hosted at seperate endpoints and follow the Kraft algorithm to handle which one is up and when. 

The script initializes a topic called 'kraken-trades' if it does not exist. Then it spins up a producer that reads from the Kraken websocket and writes to this topic and a consumer that reads from the topic and outputs the data on-read to a file available under the 'data' folder.

# Set-up
1. docker-compose down -v (if already exists)
2. docker-compose up --build

# Motivation
Done purely as an exercise to learn about websockets, streaming APIs, and integrating a reliable Kafka instance with back-ups. Most of the time spent was on finding the correct Docker image, and configuring the docker-compose.yml file with the correct broker and controller endpoints, and topic set-up. The topic sometimes takes time to create, so a bash script is written called wait-for-kafka.sh that retries topic creation till it's done and does this before spinning up the producer and consumer scripts.

![Diagram](data/diagram.png)