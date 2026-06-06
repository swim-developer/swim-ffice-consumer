package com.github.swim_developer.infrastructure.in.rest.dto;

public record SubscriptionRequest(
        String topic,
        String queueName,
        String provider,
        String description
        // TODO: Add domain-specific subscription request fields
) {}
