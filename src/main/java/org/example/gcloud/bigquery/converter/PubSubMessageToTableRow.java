package org.example.gcloud.bigquery.converter;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.example.common.BigQueryConverters;

import java.nio.charset.StandardCharsets;

/**
 * Transform PubSubMessage to Table Row using {@link BigQueryConverters#convertJsonToTableRow(String)}.
 * This step produce a multi output with successful tag {@link PubSubMessageToTableRow#TRANSFORM_OUT}
 * and error tag {@link PubSubMessageToTableRow#ERROR_MESSAGE}
 */
public class PubSubMessageToTableRow extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

    private static final String TRANSFORM_NAME = "Convert PubSub Message to Table Row";

    /** The tag for the main output of the json transformation. */
    public static final TupleTag<TableRow> TRANSFORM_OUT = new TupleTag<>();

    /** The additional tag for errors message while transformation */
    public static final TupleTag<String> ERROR_MESSAGE = new TupleTag<>();

    public PubSubMessageToTableRow() {
        super(TRANSFORM_NAME);
    }

    @Override
    public PCollectionTuple expand(PCollection<PubsubMessage> input) {
        PCollectionTuple jsonToTableRowOut = input.apply(ParDo.of(new DoFn<PubsubMessage, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                PubsubMessage message = context.element();
                String json = new String(message.getPayload(), StandardCharsets.UTF_8);
                try {
                    TableRow tableRow = BigQueryConverters.convertJsonToTableRow(json);
                    context.output(tableRow);
                } catch (Exception e) {
                    context.output(ERROR_MESSAGE, e.getMessage());
                }
            }
        }).withOutputTags(TRANSFORM_OUT, TupleTagList.of(ERROR_MESSAGE)));
        // Re-wrap the PCollections so we can return a single PCollectionTuple
        return PCollectionTuple.of(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
                .and(ERROR_MESSAGE, jsonToTableRowOut.get(ERROR_MESSAGE));
    }
}
