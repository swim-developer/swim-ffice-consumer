package com.github.swim_developer.infrastructure.out.persistence.document;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.framework.persistence.mongodb.MongoSubscriptionDocumentPort;
import io.quarkus.mongodb.panache.common.MongoEntity;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.time.Instant;

@RegisterForReflection
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "ffice_subscriptions")
public class SubscriptionDocument implements MongoSubscriptionDocumentPort {

    private ObjectId id;
    private String subscriptionId;
    private String queueName;
    private String subscriptionStatus;
    private String topic;
    private String description;
    private String type;
    private String configHash;
    private Instant subscriptionEnd;
    private String providerName;
    private String providerId;
    private String heartbeatQueue;

    // TODO: Add domain-specific subscription fields

    public static SubscriptionDocument fromDomain(Subscription subscription) {
        SubscriptionDocument doc = new SubscriptionDocument();
        doc.setId(subscription.getId() != null ? new ObjectId(subscription.getId()) : null);
        doc.setSubscriptionId(subscription.getSubscriptionId());
        doc.setQueueName(subscription.getQueueName());
        doc.setSubscriptionStatus(subscription.getSubscriptionStatus());
        doc.setTopic(subscription.getTopic());
        doc.setDescription(subscription.getDescription());
        doc.setType(subscription.getType());
        doc.setConfigHash(subscription.getConfigHash());
        doc.setSubscriptionEnd(subscription.getSubscriptionEnd());
        doc.setProviderName(subscription.getProviderName());
        doc.setProviderId(subscription.getProviderId());
        doc.setHeartbeatQueue(subscription.getHeartbeatQueue());
        // TODO: Map domain-specific fields
        return doc;
    }

    public Subscription toDomain() {
        Subscription subscription = new Subscription();
        subscription.setId(id != null ? id.toHexString() : null);
        subscription.setSubscriptionId(subscriptionId);
        subscription.setQueueName(queueName);
        subscription.setSubscriptionStatus(subscriptionStatus);
        subscription.setTopic(topic);
        subscription.setDescription(description);
        subscription.setType(type);
        subscription.setConfigHash(configHash);
        subscription.setSubscriptionEnd(subscriptionEnd);
        subscription.setProviderName(providerName);
        subscription.setProviderId(providerId);
        subscription.setHeartbeatQueue(heartbeatQueue);
        // TODO: Map domain-specific fields
        return subscription;
    }
}
