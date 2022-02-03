package org.example.gcloud.bigquery.io;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

/**
 * Write to BigQuery Table directly, instead of using the {@link TableRow}.
 * This transformation has better performance overhead, compare to
 * {@link WriteTableRowToBigQuery} because the {@link TableRow} is created just
 * before the data is pushed to BigQuery and will not be re-used.
 *
 * @see <a href='https://github.com/apache/beam/pull/4694'>This Apache Beam Pull Request<a/>
 * @param <T> the type of data which is written to BigQuery
 */
public class WriteToBigQuery<T> extends PTransform<PCollection<T>, WriteResult> {

    private static final String TRANSFORM_NAME = "Write To Big Query Table";

    private final BigQueryIO.Write<T> writeTo;

    public WriteToBigQuery(WriteToBigQueryOptions options,
                           SerializableFunction<T, TableRow> formatFunction) {
        super(TRANSFORM_NAME);
        writeTo = BigQueryIO.<T>write()
                .to(options.getOutputTableSpec())
                .withFormatFunction(formatFunction)
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withExtendedErrorInfo()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS);
    }

    @Override
    public WriteResult expand(PCollection<T> input) {
        // TODO: Upgrade this to MultiOutput with Main tag for write
        return writeTo.expand(input);
    }
}
