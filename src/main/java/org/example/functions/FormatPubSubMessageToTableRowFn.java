package org.example.functions;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Format PubSubMessage To {@link TableRow}
 */
public class FormatPubSubMessageToTableRowFn extends SimpleFunction<PubsubMessage, TableRow> {

    @Override
    public TableRow apply(PubsubMessage input) {
        try (InputStream inputStream = new ByteArrayInputStream(input.getPayload())) {
            return TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            //TODO: Better Filter or log
            throw new RuntimeException("Failed to serialize json to table row");
        }
    }

}
