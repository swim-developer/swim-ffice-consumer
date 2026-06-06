package com.github.swim_developer.application.service;

import com.github.swim_developer.application.port.out.EventStore;
import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.OutboxRouterFanOut;
import com.github.swim_developer.framework.consumer.application.messaging.processing.AbstractEventPersistenceService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.List;

@ApplicationScoped
public class EventPersistenceService extends AbstractEventPersistenceService<Event, Event> {

    private final EventStore repository;

    @Inject
    public EventPersistenceService(EventStore repository,
                                   OutboxRouterFanOut outboxRouterFanOut,
                                   SwimDeadLetterPort deadLetterService) {
        super(outboxRouterFanOut, deadLetterService);
        this.repository = repository;
    }

    @Override
    protected Event assembleEntity(ProcessingContext ctx, Event event, String contentHash) {
        event.setSubscriptionId(ctx.subscriptionId());
        event.setMessageId(ctx.amqpMessageId());
        event.setRawPayload(ctx.xmlPayload());
        event.setContentHash(contentHash);
        event.setDeliveryStatus(OutboxDeliveryStatus.SENT);
        event.setDispatchedAt(Instant.now());
        return event;
    }

    @Override
    protected void persistEntity(Event entity) { repository.persist(entity); }

    @Override
    protected void persistEntities(List<Event> entities) { repository.persist(entities); }

    @Override
    protected void updateEntity(Event entity) { repository.update(entity); }

    @Override
    protected String getServicePrefix() { return "FF-ICE"; }
}
