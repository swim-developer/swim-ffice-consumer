package com.github.swim_developer.application.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class ProcessingMetrics {

    private static final String REASON_INVALID = "INVALID";

    private final Map<String, Counter> invalidCounters = new ConcurrentHashMap<>();
    private final Counter duplicateMessagesCounter;

    @Inject
    public ProcessingMetrics(MeterRegistry meterRegistry) {
        // TODO: Define domain-specific invalid event reasons
        String[] invalidReasons = {REASON_INVALID};
        for (String reason : invalidReasons) {
            invalidCounters.put(reason, Counter.builder("ffice_events_invalid_total")
                    .tag("reason", reason)
                    .description("Total invalid FF-ICE events by reason")
                    .register(meterRegistry));
        }
        duplicateMessagesCounter = Counter.builder("ffice_duplicate_messages_total")
                .description("Total duplicate FF-ICE messages discarded")
                .register(meterRegistry);
    }

    public void incrementInvalid(String reason) {
        Counter counter = invalidCounters.get(reason);
        if (counter != null) {
            counter.increment();
        }
    }

    public void incrementDuplicate() {
        duplicateMessagesCounter.increment();
    }
}
