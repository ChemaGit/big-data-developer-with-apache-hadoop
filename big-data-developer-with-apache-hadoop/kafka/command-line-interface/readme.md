- KAFKA-TOPICS

    $ kafka-topics
    
    $ kafka-topics --zookeeper localhost:2181 --topic first_topic --create --partitions 3 --replication-factor 1
    
    $ kafka-topics --zookeeper localhost:2181 --list
    
    $ kafka-topics --zookeeper localhost:2181 --describe
    
    $ kafka-topics --zookeeper localhost:2181 --delete

- KAFKA-PRODUCER

    $ kafka-console-producer
    
    $ kafka-console-producer --broker-list localhost:9092 --topic first_topic 
    
    $ kafka-console-producer --broker-list localhost:9092 --topic first_topic --producer-property acks=all
    
    $ kafka-console-producer --broker-list localhost:9092 --topic new_topic
    
    $ kafka-topics --zookeeper localhost:2181 --list
    
    $ kafka-topics --zookeeper localhost:2181 --topic new_topic describe

- KAFKA-CONSUMER

    $ kafka-console-consumer
    
    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic
    
    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --from-beginning

- KAFKA-CONSUMERS-IN-GROUPS

    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
    
    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
    
	- The messages will be split between all consumers

    $ kafka-console-consumer --bootstrap-server localhost:9092 --topic first_topic --group my-second-application --from-beginning
    
	- The offset is commited

- KAFKA-CONSUMER-GROUPS-CLI

    $ kafka-consumer-groups --bootstrap-server localhost:9092 --list
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-second-application
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-application
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --topic first_topic --group my-first-application
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-application

- RESETTING-OFFSETS

    $ kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --execute --topic first_topic
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-first-application

    $ kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shift-by 2 --to-earliest --execute --topic first_topic
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --shift-by -2 --to-earliest --execute --topic first_topic
    
    $ kafka-consumer-groups --bootstrap-server localhost:9092 --topic first_topic --group my-first-application

- KAFKA-TOOL-UI


