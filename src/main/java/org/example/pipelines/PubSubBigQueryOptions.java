package org.example.pipelines;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.example.gcloud.bigquery.io.WriteToBigQueryOptions;
import org.example.gcloud.pubsub.io.WriteToPubSubOptions;
import org.example.gcloud.pubsub.ReadAndFilterOptions;

/**
 * Options supported by {@link PubSubBigQuery}
 */
public interface PubSubBigQueryOptions extends ReadAndFilterOptions, WriteToPubSubOptions, WriteToBigQueryOptions,
        StreamingOptions {
}
