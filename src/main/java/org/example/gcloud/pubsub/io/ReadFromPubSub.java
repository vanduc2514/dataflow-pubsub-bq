package org.example.gcloud.pubsub.io;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Read PubSub Message from a Google Pub Sub into a {@link PCollection<PubsubMessage>} by
 * {@link PubsubIO}
 */
public class ReadFromPubSub extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private static final String TRANSFORM_NAME = "Read Message from a Google Pub Sub";

    /** Begin to read PubSub Message using {@link PubsubIO} */
    private final PubsubIO.Read<PubsubMessage> readFrom;

    public ReadFromPubSub(ReadFromPubSubOptions options) {
        super(TRANSFORM_NAME);
        readFrom = PubsubIO.readMessagesWithAttributes()
                .fromSubscription(options.getInputSubscription());
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
        return readFrom.expand(input);
    }
}
