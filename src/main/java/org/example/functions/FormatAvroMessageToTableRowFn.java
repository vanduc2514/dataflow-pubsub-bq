package org.example.functions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;

/**
 * Format function which format PubSubMessage Avro Format to a TableRow
 */
public class FormatAvroMessageToTableRowFn extends SimpleFunction<PubsubMessage, TableRow> {

    private final AvroCoder<GenericRecord> coder;

    private final transient TableSchema tableSchema;

    public FormatAvroMessageToTableRowFn(Schema avroSchema) {
        super();
        coder = AvroCoder.of(GenericRecord.class, avroSchema);
        tableSchema = BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(avroSchema));
    }

    @Override
    public TableRow apply(PubsubMessage input) {
        try {
            GenericRecord avro = CoderUtils.decodeFromByteArray(coder, input.getPayload());
            return BigQueryUtils.convertGenericRecordToTableRow(avro, tableSchema);
        } catch (CoderException e) {
            //TODO: Better Filter or log
            throw new RuntimeException("Failed to serialize avro to table row");
        }
    }

}
