package org.example.functions;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * DoFn that will determine if events are to be filtered. If filtering is enabled, it will only
 * publish events that pass the filter else, it will publish all input events.
 */
@AutoValue
public abstract class ExtractAndFilterEventsFn extends DoFn<PubsubMessage, PubsubMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractAndFilterEventsFn.class);

    // Counter tracking the number of incoming Pub/Sub messages.
    private static final Counter INPUT_COUNTER =
            Metrics.counter(ExtractAndFilterEventsFn.class, "inbound-messages");

    // Counter tracking the number of output Pub/Sub messages after the user provided filter
    // is applied.
    private static final Counter OUTPUT_COUNTER =
            Metrics.counter(ExtractAndFilterEventsFn.class, "filtered-outbound-messages");

    private Boolean doFilter;
    private String inputFilterKey;
    private Pattern inputFilterValueRegex;
    private Boolean isNullFilterValue;

    public static Builder newBuilder() {
        return new AutoValue_ExtractAndFilterEventsFn.Builder();
    }

    @Nullable
    abstract ValueProvider<String> filterKey();

    @Nullable
    abstract ValueProvider<String> filterValue();

    @Setup
    public void setup() {

        if (this.doFilter != null) {
            return; // Filter has been evaluated already
        }

        inputFilterKey = (filterKey() == null ? null : filterKey().get());

        if (inputFilterKey == null) {

            // Disable input message filtering.
            this.doFilter = false;

        } else {

            this.doFilter = true; // Enable filtering.

            String inputFilterValue = (filterValue() == null ? null : filterValue().get());

            if (inputFilterValue == null) {

                LOG.warn(
                        "User provided a NULL for filterValue. Only messages with a value of NULL for the"
                                + " filterKey: {} will be filtered forward",
                        inputFilterKey);

                // For backward compatibility, we are allowing filtering by null filterValue.
                this.isNullFilterValue = true;
                this.inputFilterValueRegex = null;
            } else {

                this.isNullFilterValue = false;
                try {
                    inputFilterValueRegex = getFilterPattern(inputFilterValue);
                } catch (PatternSyntaxException e) {
                    LOG.error("Invalid regex pattern for supplied filterValue: {}", inputFilterValue);
                    throw new RuntimeException(e);
                }
            }

            LOG.info(
                    "Enabling event filter [key: " + inputFilterKey + "][value: " + inputFilterValue + "]");
        }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {

        INPUT_COUNTER.inc();
        if (Boolean.FALSE.equals(this.doFilter)) {

            // Filter is not enabled
            writeOutput(context, context.element());
        } else {

            PubsubMessage message = context.element();
            String extractedValue = message.getAttribute(this.inputFilterKey);

            if (Boolean.TRUE.equals(this.isNullFilterValue)) {

                if (extractedValue == null) {
                    // If we are filtering for null and the extracted value is null, we forward
                    // the message.
                    writeOutput(context, message);
                }

            } else {

                if (extractedValue != null
                        && this.inputFilterValueRegex.matcher(extractedValue).matches()) {
                    // If the extracted value is not null and it matches the filter,
                    // we forward the message.
                    writeOutput(context, message);
                }
            }
        }
    }

    /**
     * Write a {@link PubsubMessage} and increment the output counter.
     *
     * @param context {@link ProcessContext} to write {@link PubsubMessage} to.
     * @param message {@link PubsubMessage} output.
     */
    private void writeOutput(ProcessContext context, PubsubMessage message) {
        OUTPUT_COUNTER.inc();
        context.output(message);
    }

    /**
     * Return a {@link Pattern} based on a user provided regex string.
     *
     * @param regex Regex string to compile.
     * @return {@link Pattern}
     * @throws PatternSyntaxException If the string is an invalid regex.
     */
    private Pattern getFilterPattern(String regex) throws PatternSyntaxException {
        checkNotNull(regex, "Filter regex cannot be null.");
        return Pattern.compile(regex);
    }

    /** Builder class for {@link ExtractAndFilterEventsFn}. */
    @AutoValue.Builder
    public abstract static class Builder {

        abstract Builder setFilterKey(ValueProvider<String> filterKey);

        abstract Builder setFilterValue(ValueProvider<String> filterValue);

        public abstract ExtractAndFilterEventsFn build();

        /**
         * Method to set the filterKey used for filtering messages.
         *
         * @param filterKey Lookup key for the {@link PubsubMessage} attribute map.
         * @return {@link Builder}
         */
        public Builder withFilterKey(ValueProvider<String> filterKey) {
            checkArgument(filterKey != null, "withFilterKey(filterKey) called with null input.");
            return setFilterKey(filterKey);
        }

        /**
         * Method to set the filterValue used for filtering messages.
         *
         * @param filterValue Lookup value for the {@link PubsubMessage} attribute map.
         * @return {@link Builder}
         */
        public Builder withFilterValue(ValueProvider<String> filterValue) {
            checkArgument(filterValue != null, "withFilterValue(filterValue) called with null input.");
            return setFilterValue(filterValue);
        }
    }
}
