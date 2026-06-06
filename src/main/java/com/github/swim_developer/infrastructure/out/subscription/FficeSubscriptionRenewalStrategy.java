package com.github.swim_developer.infrastructure.out.subscription;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.application.port.out.SubscriptionStore;
import com.github.swim_developer.infrastructure.out.client.SubscriptionManagerAdapter;
import com.github.swim_developer.infrastructure.out.client.SubscriptionManagerRestClient;
import com.github.swim_developer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.framework.consumer.infrastructure.out.config.provider.ProviderConfigParser;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.domain.model.SubscriptionRenewalInfo;
import com.github.swim_developer.framework.domain.exception.SubscriptionRenewalException;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import com.github.swim_developer.framework.application.port.out.SubscriptionRenewalStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

@Slf4j
@ApplicationScoped
public class FficeSubscriptionRenewalStrategy implements SubscriptionRenewalStrategy {

    private final SubscriptionStore subscriptionStore;
    private final SubscriptionManagerAdapter smClientRegistry;
    private final ProviderConfigParser providerConfigParser;

    @Inject
    public FficeSubscriptionRenewalStrategy(SubscriptionStore subscriptionStore,
                                            SubscriptionManagerAdapter smClientRegistry,
                                            ProviderConfigParser providerConfigParser) {
        this.subscriptionStore = subscriptionStore;
        this.smClientRegistry = smClientRegistry;
        this.providerConfigParser = providerConfigParser;
    }

    @Override
    public List<SubscriptionRenewalInfo> findSubscriptionsNearExpiry(Instant threshold) {
        return subscriptionStore.findBySubscriptionEndBefore(threshold)
                .stream()
                .filter(sub -> SubscriptionStatus.ACTIVE.name().equals(sub.getSubscriptionStatus()))
                .map(sub -> new SubscriptionRenewalInfo(sub.getSubscriptionId(), sub.getSubscriptionEnd()))
                .toList();
    }

    @Override
    public void renewSubscription(String subscriptionId) throws SubscriptionRenewalException {
        log.info("Renewing subscription: {}", subscriptionId);

        Subscription subscription = subscriptionStore.findBySubscriptionId(subscriptionId)
                .orElseThrow(() -> new IllegalStateException("Subscription not found: " + subscriptionId));

        SubscriptionManagerRestClient client = resolveSmClient(subscription.getProviderId());
        SubscriptionResponse response = client.renewSubscription(subscriptionId);

        subscription.setSubscriptionEnd(response.subscriptionEnd());
        subscriptionStore.updateSubscription(subscription);

        log.info("Subscription renewed - ID: {}, New end: {}", subscriptionId, response.subscriptionEnd());
    }

    private SubscriptionManagerRestClient resolveSmClient(String providerId) {
        ProviderConfiguration provider = providerConfigParser.findByProviderId(providerId)
                .orElseThrow(() -> new IllegalStateException("Provider not configured: " + providerId));
        return smClientRegistry.getOrCreate(provider);
    }
}
