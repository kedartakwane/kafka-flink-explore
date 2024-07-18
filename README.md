# Exploring Kafka Flink

## Data Pipeline
![Arch](/pipeline-architecture.png)

## Kafka
## Setup
- Check [docker-compose.yml](/docker-compose.yml)

## Flink 
### Setup
#### Locally
- Download Apache Flink from https://flink.apache.org/downloads/
- Extract the compressed file
- Go to the root folder
- Can change the config file `flink_root/conf/flink-conf.yml` like adding more increasing `taskmanager.numberOfTaskSlots` and `parallelism.default` properties.
- Start the cluster:
```bash
./bin/start-cluster.sh
```
- Open the Flink Dashboard: http://localhost:8081
- Stop the cluster:
```bash
./bin/stop-cluster.sh
``` 
#### Docker
- Check the [docker-compose.yml](/docker-compose.yml)

### Creating project using Intellij
Follow this:
![Creating new Flink Project](/extras/images/Flink-Project-Creation.png)

### Testing
- Make sure the the Flink cluster is up and running
- Create jar:
```bash
# Erase target folder
mvn clean
# Compile to check for error
mvn compile
# Create jar
mvn package
```
- Running the Flink job:
```bash
[path/to/downloaded_flink_in_setup] run -c [packageName.JavaFileName] [path/to/jar]

# Sample
/Users/kedartakwane/Developer/local/flink/flink-1.18.1/bin/flink run -c flinkExplore.DataStreamJob target/flink-data-processing-1.0-SNAPSHOT.jar
```