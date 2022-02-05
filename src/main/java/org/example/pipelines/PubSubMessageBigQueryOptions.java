package org.example.pipelines;

import org.apache.beam.sdk.options.StreamingOptions;
import org.example.gcloud.bigquery.io.WriteToBigQueryOptions;
import org.example.gcloud.pubsub.io.WriteToPubSubOptions;
import org.example.gcloud.pubsub.ReadAndFilterOptions;

/**
 * Options supported by {@link PubSubMessageBigQuery}
 */
public interface PubSubMessageBigQueryOptions extends ReadAndFilterOptions, WriteToPubSubOptions, WriteToBigQueryOptions,
        StreamingOptions {
}
