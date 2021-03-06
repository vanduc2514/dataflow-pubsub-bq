package org.example.gcloud.pubsub.io;

import org.apache.beam.sdk.options.*;

/**
 * Options used for {@link WritePubSubMessage}, which publish
 * to a Google Cloud Topic
 */
public interface WriteToPubSubOptions extends PipelineOptions {

    @Description(
            "The Cloud Pub/Sub topic to publish to. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> outputTopic);

}
