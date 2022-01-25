package org.example.gcloud.pubsub.message;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.functions.ExtractAndFilterEventsFn;

/**
 * Filter the {@link PubsubMessage} from a {@link PCollection<PubsubMessage>}
 */
public class FilterMessage extends PTransform<PCollection<? extends PubsubMessage>, PCollection<PubsubMessage>> {

    private static final String TRANSFORM_NAME = "Filter PubSub Message";

    private final ParDo.SingleOutput<PubsubMessage, PubsubMessage> filterTransform;

    public FilterMessage(FilterMessageOptions options) {
        super(TRANSFORM_NAME);
        var doFn = ExtractAndFilterEventsFn.newBuilder()
                .withFilterKey(options.getFilterKey())
                .withFilterValue(options.getFilterValue())
                .build();
        filterTransform = ParDo.of(doFn);
    }

    @Override
    public PCollection<PubsubMessage> expand(PCollection<? extends PubsubMessage> input) {
        return filterTransform.expand(input);
    }
}
