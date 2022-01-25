package org.example.gcloud.pubsub;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.example.gcloud.pubsub.message.FilterMessage;
import org.example.gcloud.pubsub.io.ReadFromPubSub;

/**
 * Composite Transformation for reading and filter the messages from a Google Cloud Subscription
 *
 * @see <a href="https://beam.apache.org/documentation/programming-guide/#composite-transforms">
 *          Apache Beam Composite Transforms
 *     </a>
 */
public class ReadAndFilterPubSub extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private static final String TRANSFORM_NAME = "Read from PubSub then Filter";

    private final ReadFromPubSub inputTransform;

    private final FilterMessage filterTransform;

    public ReadAndFilterPubSub(ReadAndFilterOptions options) {
        super(TRANSFORM_NAME);
        inputTransform = new ReadFromPubSub(options);
        filterTransform = new FilterMessage(options);
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
        // 1. Read PubSubMessage with attributes from input PubSub subscription
        PCollection<PubsubMessage> receivedMessage = inputTransform.expand(input);
        // 2. Apply any filters if an attribute=value pair is provided otherwise, return all messages
        final PCollection<PubsubMessage> filteredMessage = receivedMessage.apply(filterTransform);
        return filteredMessage;
    }

}
