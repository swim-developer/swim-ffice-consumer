package com.github.swim_developer.application.port.in;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.domain.model.command.SubscriptionCommand;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;

import java.util.Optional;

public interface ManageSubscriptionPort {

    Subscription createSubscription(SubscriptionCommand command);

    Optional<Subscription> findBySubscriptionId(String subscriptionId);

    void deleteSubscriptionById(String subscriptionId);

    Subscription pauseSubscription(String subscriptionId);

    Subscription resumeSubscription(String subscriptionId);

    ProviderConfiguration resolveProvider(String providerId);
}
