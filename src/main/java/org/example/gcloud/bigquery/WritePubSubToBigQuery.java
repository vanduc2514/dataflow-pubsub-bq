package org.example.gcloud.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.example.gcloud.bigquery.converter.PubSubMessageToTableRow;
import org.example.gcloud.bigquery.io.WriteTableRowToBigQuery;
import org.example.gcloud.bigquery.io.WriteToBigQuery;
import org.example.gcloud.bigquery.io.WriteToBigQueryOptions;

/**
 * Composite Transformation for converting a {@link PubsubMessage} to a {@link TableRow} then
 * write to a Big Query Table.
 *
 * @see <a href="https://beam.apache.org/documentation/programming-guide/#composite-transforms">
 *          Apache Beam Composite Transforms
 *     </a>
 */
public class WritePubSubToBigQuery extends PTransform<PCollection<PubsubMessage>, WriteResult> {

    private static final String TRANSFORM_NAME = "Write PubSub Message to BigQuery";

    private final PubSubMessageToTableRow convertPubSubToTableRow;

    private final WriteTableRowToBigQuery writeToBigQuery;

    private WriteToBigQuery<PubsubMessage> latestWrite;

    public WritePubSubToBigQuery(WriteToBigQueryOptions options) {
        super(TRANSFORM_NAME);
        convertPubSubToTableRow = new PubSubMessageToTableRow();
        writeToBigQuery = new WriteTableRowToBigQuery(options);
        latestWrite = new WriteToBigQuery<>(
                options, message -> new TableRow()
        );
    }

    @Override
    public WriteResult expand(PCollection<PubsubMessage> input) {
        latestWrite.expand(input);
        // 1. Convert PubSub Message
        PCollectionTuple tableRowTuple = convertPubSubToTableRow.expand(input);
        // 2. Write to Big Query Table
        WriteResult writeResult = tableRowTuple.get(PubSubMessageToTableRow.TRANSFORM_OUT).apply(writeToBigQuery);
        return writeResult;
    }

}
