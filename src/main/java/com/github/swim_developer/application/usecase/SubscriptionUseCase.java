package com.github.swim_developer.application.usecase;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.domain.model.command.SubscriptionCommand;
import com.github.swim_developer.application.port.in.ManageSubscriptionPort;
import com.github.swim_developer.application.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.application.port.out.SubscriptionStore;
import com.github.swim_developer.framework.domain.exception.SubscriptionNotFoundException;
import com.github.swim_developer.framework.consumer.application.subscription.service.AbstractSubscriptionService;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import com.github.swim_developer.framework.application.port.out.SwimConsumerManagerPort;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

import static com.github.swim_developer.framework.domain.model.SubscriptionType.DECLARED;
import static com.github.swim_developer.framework.domain.model.SubscriptionType.ON_DEMAND;

@Slf4j
@ApplicationScoped
public class SubscriptionUseCase extends AbstractSubscriptionService<SubscriptionCommand, Subscription>
        implements ManageSubscriptionPort {

    private final SubscriptionStore repository;
    private final RemoteSubscriptionManagerPort smPort;
    private final SwimProviderConfigPort providerConfigParser;
    private final SwimSubscriptionFilterPort filterCache;

    SubscriptionUseCase() {
        this.repository = null;
        this.smPort = null;
        this.providerConfigParser = null;
        this.filterCache = null;
    }

    @Inject
    public SubscriptionUseCase(SubscriptionStore repository,
                               RemoteSubscriptionManagerPort smPort,
                               SwimProviderConfigPort providerConfigParser,
                               SwimConsumerManagerPort consumerManager,
                               SwimSubscriptionFilterPort filterCache) {
        super(consumerManager);
        this.repository = repository;
        this.smPort = smPort;
        this.providerConfigParser = providerConfigParser;
        this.filterCache = filterCache;
    }

    @Override
    public void resetAllSubscriptions(boolean deleteAndRecreate) {
        if (deleteAndRecreate) {
            repository.deleteAllSubscriptions();
            filterCache.clear();
            log.info("All subscriptions deleted (delete-and-recreate mode)");
        }
    }

    @Override
    protected List<Subscription> findActiveSubscriptions() {
        return repository.findActiveSubscriptions();
    }

    @Override
    protected void onAllConsumersRegistered() {
        populateFilterCache();
    }

    @Override
    public void populateFilterCache() {
        repository.findActiveSubscriptions().forEach(this::cacheFilters);
        log.info("Subscription filter cache populated with {} entries", filterCache.size());
    }

    @Override
    public Subscription createSubscription(SubscriptionCommand command) {
        String configHash = command.generateConfigHash();

        Optional<Subscription> existing = repository.findByConfigHash(configHash);
        if (existing.isPresent()) {
            log.warn("Subscription already exists: {}", existing.get().getSubscriptionId());
            return existing.get();
        }

        ProviderConfiguration provider = resolveProvider(command.provider());
        Subscription subscription = smPort.createSubscription(command, provider);

        subscription.setType(ON_DEMAND.name());
        subscription.setConfigHash(configHash);
        // TODO: Apply domain-specific filter fallback from command to subscription
        repository.persistSubscription(subscription);

        cacheFilters(subscription);
        activateSubscription(subscription.getSubscriptionId(), subscription.getQueueName(), provider);
        return repository.findBySubscriptionId(subscription.getSubscriptionId()).orElse(subscription);
    }

    @Override
    protected Subscription callCreateAndPersist(SubscriptionCommand desired) {
        String configHash = desired.generateConfigHash();
        ProviderConfiguration provider = resolveProvider(desired.provider());
        Subscription remote = smPort.createSubscription(desired, provider);

        Optional<Subscription> duplicate = repository.findBySubscriptionId(remote.getSubscriptionId());
        if (duplicate.isPresent()) {
            Subscription existing = duplicate.get();
            existing.setQueueName(remote.getQueueName());
            existing.setSubscriptionStatus(remote.getSubscriptionStatus());
            existing.setConfigHash(configHash);
            existing.setProviderId(desired.provider());
            repository.updateSubscription(existing);
            cacheFilters(existing);
            return existing;
        }

        remote.setType(DECLARED.name());
        remote.setConfigHash(configHash);
        repository.persistSubscription(remote);
        cacheFilters(remote);
        return remote;
    }

    @Override
    protected String callUpdateStatus(String subscriptionId, String newStatus) {
        try {
            ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
            return smPort.updateSubscriptionStatus(subscriptionId, newStatus, provider);
        } catch (WebApplicationException e) {
            int status = e.getResponse().getStatus();
            if (status == 404 || status == 410) {
                throw new SubscriptionNotFoundException(subscriptionId, e);
            }
            throw e;
        }
    }

    @Override
    protected void callDeleteRemoteSubscription(String subscriptionId) {
        ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
        smPort.deleteSubscription(subscriptionId, provider);
    }

    @Override
    protected boolean existsLocally(SubscriptionCommand desired) {
        return repository.findByConfigHashAndType(desired.generateConfigHash(), DECLARED.name()).isPresent();
    }

    @Override
    public Optional<Subscription> findBySubscriptionId(String subscriptionId) {
        return repository.findBySubscriptionId(subscriptionId);
    }

    @Override
    protected List<Subscription> loadDeclaredSubscriptions() {
        return repository.findDeclaredSubscriptions();
    }

    @Override
    protected boolean isStillDesired(Subscription current, List<SubscriptionCommand> desiredSubscriptions) {
        return desiredSubscriptions.stream()
                .anyMatch(desired -> desired.generateConfigHash().equals(current.getConfigHash()));
    }

    @Override
    protected void deleteLocalSubscription(String subscriptionId) {
        filterCache.removeSubscription(subscriptionId);
        repository.deleteBySubscriptionId(subscriptionId);
    }

    @Override
    protected void updateLocalStatus(String subscriptionId, String status) {
        repository.updateStatus(subscriptionId, status);
    }

    @Override
    protected String describeDesired(SubscriptionCommand desired) {
        return desired.description();
    }

    @Override
    protected Optional<SubscriptionCommand> toDesiredConfig(Subscription subscription) {
        return Optional.of(new SubscriptionCommand(
                subscription.getTopic(),
                null,
                subscription.getProviderId(),
                subscription.getDescription(),
                subscription.getMessageTypes() != null ? subscription.getMessageTypes() : List.of(),
                subscription.getAerodromes() != null ? subscription.getAerodromes() : List.of()
        ));
    }

    @Override
    public ProviderConfiguration resolveProvider(String providerId) {
        return providerConfigParser.findByProviderIdOrDefault(providerId)
                .orElseThrow(() -> new IllegalStateException("Provider not configured: " + providerId));
    }

    private ProviderConfiguration resolveProviderForSubscription(String subscriptionId) {
        Subscription subscription = repository.findBySubscriptionId(subscriptionId)
                .orElseThrow(() -> new IllegalArgumentException("Subscription not found: " + subscriptionId));
        return resolveProvider(subscription.getProviderId());
    }

    // TODO: Implement domain-specific filter caching based on your FilterDimension constants
    private void cacheFilters(Subscription subscription) {
    }
}
