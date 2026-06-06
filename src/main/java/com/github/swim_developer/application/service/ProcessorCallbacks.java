package com.github.swim_developer.application.service;

import com.github.swim_developer.application.port.out.SubscriptionStore;
import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventProcessorCallbacks;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventProcessorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
@ApplicationScoped
public class ProcessorCallbacks implements SwimEventProcessorCallbacks<Event> {

    private final ProcessingMetrics metrics;
    private final SubscriptionStore subscriptionStore;

    @Inject
    public ProcessorCallbacks(ProcessingMetrics metrics, SubscriptionStore subscriptionStore) {
        this.metrics = metrics;
        this.subscriptionStore = subscriptionStore;
    }

    @Override
    public boolean preProcess(ProcessingContext ctx) {
        Optional<Subscription> sub = subscriptionStore.findBySubscriptionId(ctx.subscriptionId());
        if (sub.isPresent() && "PAUSED".equals(sub.get().getSubscriptionStatus())) {
            log.warn("PAUSED_SUBSCRIPTION_DISCARD: SubscriptionId={}, MessageId={}",
                    ctx.subscriptionId(), ctx.amqpMessageId());
            return true;
        }
        return false;
    }

    @Override
    public void onDuplicateDetected(ProcessingContext ctx, String contentHash) {
        metrics.incrementDuplicate();
    }

    @Override
    public void onExtractionFailure(ProcessingContext ctx, SwimEventProcessorConfig config) {
        metrics.incrementInvalid("INVALID");
        log.error("Invalid FF-ICE message - MessageId: {}", ctx.compositeMessageId());
    }

    @Override
    public void onValidationFailure(ProcessingContext ctx, Exception e) {
        log.error("Problematic XML (first 500 chars): {}",
                ctx.xmlPayload().length() > 500 ? ctx.xmlPayload().substring(0, 500) : ctx.xmlPayload());
    }
}
