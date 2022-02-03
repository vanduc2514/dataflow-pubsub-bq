package org.example.gcloud.bigquery.io;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Options use for {@link WriteTableRowToBigQuery} transformation,
 * which write to a Google Cloud Big Query Table
 */
public interface WriteToBigQueryOptions extends PipelineOptions {

    @Description("Table spec to write the output to. " +
            "The name should be in the format of " +
            "<project-id>:<dataset>.<table-name>")
    ValueProvider<String> getOutputTableSpec();

    void setOutputTableSpec(ValueProvider<String> value);

}
