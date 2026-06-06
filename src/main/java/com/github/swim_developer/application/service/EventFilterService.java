package com.github.swim_developer.application.service;

import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import com.github.swim_developer.framework.consumer.application.messaging.processing.AbstractEventFilterService;
import com.github.swim_developer.framework.consumer.application.messaging.processing.FilterRule;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class EventFilterService extends AbstractEventFilterService<Event> {

    @Inject
    public EventFilterService(SwimSubscriptionFilterPort filterCache,
                              SwimDeadLetterPort deadLetterService) {
        super(filterCache, deadLetterService);
    }

    @Override
    protected List<FilterRule<Event>> buildFilterRules(Event event) {
        return List.of();
    }
}
