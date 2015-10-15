This program connects to a Kafka Broker through Spark Stream and downloads messages which are in JSON format
Once downloaded , it generates the titles which are in failed list, like an array of the values. Then does a group by on the titles to count/ aggregate the total failures
