package com.github.swim_developer.application.port.out;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.domain.model.command.SubscriptionCommand;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;

public interface RemoteSubscriptionManagerPort {

    Subscription createSubscription(SubscriptionCommand command, ProviderConfiguration provider);

    String updateSubscriptionStatus(String subscriptionId, String newStatus, ProviderConfiguration provider);

    void deleteSubscription(String subscriptionId, ProviderConfiguration provider);
}
