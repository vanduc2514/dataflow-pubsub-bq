package org.example.gcloud.pubsub.message;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Options for {@link FilterMessage}, which filter the PubSub Message
 */
public interface FilterMessageOptions extends PipelineOptions {
    @Description(
            "Filter events based on an optional attribute key. "
                    + "No filters are applied if a filterKey is not specified.")
    @Validation.Required
    ValueProvider<String> getFilterKey();

    void setFilterKey(ValueProvider<String> filterKey);

    @Description(
            "Filter attribute value to use in case a filterKey is provided. Accepts a valid Java regex"
                    + " string as a filterValue. In case a regex is provided, the complete expression"
                    + " should match in order for the message to be filtered. Partial matches (e.g."
                    + " substring) will not be filtered. A null filterValue is used by default.")
    @Validation.Required
    ValueProvider<String> getFilterValue();

    void setFilterValue(ValueProvider<String> filterValue);
}
