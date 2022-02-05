package org.example.pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.example.functions.FormatPubSubMessageToTableRowFn;
import org.example.gcloud.bigquery.io.WriteToBigQuery;
import org.example.gcloud.pubsub.ReadAndFilterPubSub;
import org.example.gcloud.pubsub.io.WritePubSubMessage;

/**
 * Pipeline for
 * 1. Filter Read Json Message from a PubSub
 * 2.1.p Parallel write to a BigQuery Table
 * 2.2.p Parallel write to another PubSub
 *
 * # Build the template and execute on the Dataflow Runner with the following command
 * mvn compile exec:java \
 * -Dexec.mainClass=org.example.pipelines.PubSubBigQuery \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --gcpStagingLocation=gs://${PROJECT_CLOUD_STORAGE}/staging \
 * --gcpTempLocation=gs://${PROJECT_CLOUD_STORAGE}/temp \
 * --templateLocation=${PIPELINE_FOLDER}/template \
 * --region=${REGION_NAME} \
 * --subnetwork=${PROJECT_VPC_SUBNETWORK} \
 * --runner=DataflowRunner \
 * --inputSubscription=projects/${PROJECT_ID}/subscriptions/${INPUT_SUBSCRIPTION_NAME} \
 * --outputTopic=projects/${PROJECT_ID}/topics/${OUTPUT_TOPIC_NAME} \
 * --outputTableSpec=${PROJECT_ID}:${DATASET}.${TABLE_NAME} \
 * "
 */
public class PubSubMessageBigQuery {

    /**
     * Main entry point for executing the pipeline.
     *
     * @param args The command-line arguments to the pipeline.
     */
    public static void main(String[] args) {

        // Parse the user options passed from the command-line
        PubSubMessageBigQueryOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PubSubMessageBigQueryOptions.class);
        // Set the pipeline to do streaming processing
        options.setStreaming(true);

        // Create the pipeline with the given options
        Pipeline pipeline = Pipeline.create(options);
        // Read and filter (if an attribute=value pair is provided) from a GC Subscription
        final PCollection<PubsubMessage> receivedMessage = pipeline.apply(
                new ReadAndFilterPubSub(options)
        );
        // Branching PCollections to perform Parallel process
        // see https://beam.apache.org/documentation/pipelines/design-your-pipeline/
        // Parallel write PubSub message to BigQuery
        receivedMessage.apply(new WriteToBigQuery<>(options, new FormatPubSubMessageToTableRowFn()));
        // Parallel write PubSub message to another PubSub.
        receivedMessage.apply(new WritePubSubMessage(options));

        // Do execute the pipeline
        pipeline.run();
    }

}