package com.github.swim_developer.infrastructure.out.mapper;

import com.github.swim_developer.framework.consumer.infrastructure.out.dlq.DeadLetterMessage;
import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.infrastructure.in.rest.dto.EventDTO;
import com.github.swim_developer.framework.infrastructure.out.messaging.DlqMessageDTO;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class SubscriptionMapper {

    public EventDTO toDTO(Event event) {
        return new EventDTO(
                event.getId() != null ? event.getId().toHexString() : null,
                event.getMessageId(),
                event.getSubscriptionId(),
                event.getReceivedAt(),
                event.getFficeMessageType(),
                event.getGufi(),
                event.getAircraftIdentification(),
                event.getDepartureAerodrome(),
                event.getArrivalAerodrome()
        );
    }

    public DlqMessageDTO toDTO(DeadLetterMessage dlq) {
        return new DlqMessageDTO(
                dlq.getId(),
                dlq.getAmqpMessageId(),
                dlq.getMessageIndex(),
                dlq.getSubscriptionId(),
                dlq.getQueueName(),
                dlq.getErrorType(),
                dlq.getErrorMessage(),
                dlq.getRawPayload(),
                dlq.getReceivedAt(),
                dlq.getFailedAt()
        );
    }
}
