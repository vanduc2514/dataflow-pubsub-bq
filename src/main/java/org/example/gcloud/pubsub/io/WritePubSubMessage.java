package org.example.gcloud.pubsub.io;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Write Message to a Google Cloud PubSub Topic using {@link PubsubIO}
 */
public class WritePubSubMessage extends PTransform<PCollection<PubsubMessage>, PDone> {

    private static final String TRANSFORM_NAME = "Write PubSub Message To Pub Sub";

    private final PubsubIO.Write<PubsubMessage> writeTo;

    public WritePubSubMessage(@NonNull WriteToPubSubOptions options) {
        super(TRANSFORM_NAME);
        writeTo = PubsubIO.writeMessages().to(options.getOutputTopic());
    }

    @Override
    public PDone expand(PCollection<PubsubMessage> input) {
        return writeTo.expand(input);
    }

}
