This project is build to monitor various accessories of hadoop like Kafka Borkers, Zookeepers, Star Gate Rest API's, Hive Server 2 etc
The dependency libs are added under the txt file Dependency.txt
The progremme read's a lookup file to understand the hots and port for various services , information about what to monitor
The MainMonitor takes the argument of the path where the lookfiles can be found and that should be good enough to start it.
At the end of each programme , the data is uploaded to mysql database 
