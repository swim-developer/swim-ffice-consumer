package com.github.swim_developer.application.port.out;

import com.github.swim_developer.domain.model.Subscription;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SubscriptionStore {

    void persistSubscription(Subscription subscription);

    void updateSubscription(Subscription subscription);

    void updateStatus(String subscriptionId, String newStatus);

    Optional<Subscription> findBySubscriptionId(String subscriptionId);

    List<Subscription> findAllSubscriptions();

    List<Subscription> findActiveSubscriptions();

    List<Subscription> findDeclaredSubscriptions();

    List<Subscription> findBySubscriptionEndBefore(Instant threshold);

    Optional<Subscription> findByConfigHash(String configHash);

    Optional<Subscription> findByConfigHashAndType(String configHash, String type);

    Optional<Subscription> findByQueueName(String queueName);

    boolean deleteBySubscriptionId(String subscriptionId);

    long countSubscriptions();

    void deleteAllSubscriptions();
}
