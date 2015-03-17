# kafka-dispatch

One might ask "what can this project can do me?", to which the answer is

 * forward stdin line by line to Kafka 
 * (yes, that is it)


### Requirements

 * java 8


### Building it

 * ````mvn clean package````


### Usage

```
$ java -jar target/kafka-dispatch-1.0-SNAPSHOT.jar --help
Usage: <main class> [options]
  Options:
    -d, --dump-stats
       print stats when done
       Default: false
    -h, --help
       display this help message
       Default: false
  * -s, --servers
       servers to connect to, delimited by comma
  * -t, --topic
       topic to write to
    -v
       turn on output
       Default: false
```

```
$ java -jar target/kafka-dispatch-1.0-SNAPSHOT.jar -s kafka-host-1:9092,kafka-host-2:9092 -t my-topic < /my/data/file
```


### Feedback

Please use the github issue-tracker for submitting bug reports and suggestions.
