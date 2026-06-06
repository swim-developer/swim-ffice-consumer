package com.github.swim_developer.domain.model;

import com.github.swim_developer.framework.domain.model.SwimConsumerSubscription;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@RegisterForReflection
public class Subscription implements SwimConsumerSubscription {

    private String id;
    private String subscriptionId;
    private String queueName;
    private String subscriptionStatus;
    private String description;
    private String type;
    private String configHash;
    private Instant subscriptionEnd;
    private String providerName;
    private String providerId;
    private String heartbeatQueue;
    private String topic;
    private List<String> aerodromes;
    private List<String> messageTypes;

    @Override
    public Map<String, Set<String>> projectFilterDimensions() {
        // TODO: Implement filter dimension projection for your domain
        return Map.of();
    }
}
