package com.github.swim_developer.application.service;

import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventValidator;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class EventDataValidator implements SwimEventValidator<Event> {

    @Override
    public void validateExtractedData(ProcessingContext ctx, Event event) {
        if (event.getFficeMessageType() == null || event.getFficeMessageType().isBlank()) {
            log.warn("FF-ICE message type is missing - MessageId: {}", ctx.compositeMessageId());
        }
    }
}
