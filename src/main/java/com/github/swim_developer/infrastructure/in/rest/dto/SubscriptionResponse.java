package com.github.swim_developer.infrastructure.in.rest.dto;

import java.time.Instant;

public record SubscriptionResponse(
        String subscriptionId,
        String subscriptionStatus,
        String queueName,
        Instant subscriptionEnd
) {}
