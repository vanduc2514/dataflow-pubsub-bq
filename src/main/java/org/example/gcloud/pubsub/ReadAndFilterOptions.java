package org.example.gcloud.pubsub;

import org.example.gcloud.pubsub.io.ReadFromPubSubOptions;
import org.example.gcloud.pubsub.message.FilterMessageOptions;

/**
 * Options for {@link ReadAndFilterPubSub},
 * which fetch and filter messages from a Google Cloud Subscription
 */
public interface ReadAndFilterOptions extends ReadFromPubSubOptions, FilterMessageOptions {
}
