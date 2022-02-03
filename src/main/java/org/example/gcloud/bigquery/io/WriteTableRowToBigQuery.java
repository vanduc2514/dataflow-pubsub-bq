package org.example.gcloud.bigquery.io;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class WriteTableRowToBigQuery extends PTransform<PCollection<TableRow>, WriteResult> {

    private static final String TRANSFORM_NAME = "Write To Big Query Table";

    private final BigQueryIO.Write<TableRow> writeTo;

    public WriteTableRowToBigQuery(WriteToBigQueryOptions options) {
        super(TRANSFORM_NAME);
        writeTo = BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                .to(options.getOutputTableSpec());
    }

    @Override
    public WriteResult expand(PCollection<TableRow> input) {
        // TODO: Upgrade this to MultiOutput with Main tag for successful write
        // And additional tag for error write
        return writeTo.expand(input);
    }
}
