# Dataflow Examples

## Build this project to a jar

```shell
mvn clean install
```

## Execute Individual Pipelines
### PubSub BigQuery
This pipeline will Read and Filter from a Google PubSub, then
write to another PubSub and write to a BQ Table in parallel

Export the required variable 
```shell
    export PROJECT_ID=
    export PROJECT_CLOUD_STORAGE=
    export REGION_NAME=
    export PROJECT_VPC_SUBNETWORK=
    export INPUT_SUBSCRIPTION_NAME=
    export OUTPUT_TOPIC_NAME=
    export DATASET=
    export TABLE_NAME=
```

Execute the pipeline
```shell
    mvn compile exec:java \
    -Dexec.mainClass=org.example.pipelines.PubSubMessageBigQuery \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
    --project=${PROJECT_ID} \
    --gcpStagingLocation=gs://${PROJECT_CLOUD_STORAGE}/staging \
    --gcpTempLocation=gs://${PROJECT_CLOUD_STORAGE}/temp \
    --region=${REGION_NAME} \
    --subnetwork=${PROJECT_VPC_SUBNETWORK} \
    --runner=DataflowRunner \
    --inputSubscription=projects/${PROJECT_ID}/subscriptions/${INPUT_SUBSCRIPTION_NAME} \
    --outputTopic=projects/${PROJECT_ID}/topics/${OUTPUT_TOPIC_NAME} \
    --outputTableSpec=${PROJECT_ID}:${DATASET}.${TABLE_NAME} \
    "
```
