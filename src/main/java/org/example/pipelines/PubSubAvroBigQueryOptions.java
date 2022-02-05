package org.example.pipelines;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.example.gcloud.bigquery.io.WriteToBigQueryOptions;
import org.example.gcloud.pubsub.ReadAndFilterOptions;
import org.example.gcloud.pubsub.io.WriteToPubSubOptions;

public interface PubSubAvroBigQueryOptions extends ReadAndFilterOptions, WriteToPubSubOptions, WriteToBigQueryOptions,
        StreamingOptions {

    @Description("User provided AVRO schema")
    @Validation.Required
    String getAvroSchema();

    void setAvroSchema(String avroSchema);

}
