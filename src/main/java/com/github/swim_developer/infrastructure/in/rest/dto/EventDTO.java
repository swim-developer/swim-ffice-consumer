package com.github.swim_developer.infrastructure.in.rest.dto;

import java.time.Instant;

public record EventDTO(
        String id,
        String messageId,
        String subscriptionId,
        Instant receivedAt,
        String fficeMessageType,
        String gufi,
        String aircraftIdentification,
        String departureAerodrome,
        String arrivalAerodrome
) {}
