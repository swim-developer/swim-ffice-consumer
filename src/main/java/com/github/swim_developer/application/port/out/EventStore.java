package com.github.swim_developer.application.port.out;

import com.github.swim_developer.domain.model.Event;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface EventStore {

    List<Event> listAllDomain();

    Optional<Event> findById(String id);

    Optional<Event> findByMessageId(String messageId);

    boolean existsByContentHash(String contentHash);

    long countAll();

    void persist(Event event);

    void persist(List<Event> events);

    void update(Event event);

    List<Event> findBySubscriptionIdPaginated(String subscriptionId, int page, int size);

    long countBySubscriptionId(String subscriptionId);

    List<Event> findBySubscriptionIdAndDateRange(String subscriptionId, Instant startDate, Instant endDate, int page, int size);

    long countBySubscriptionIdAndDateRange(String subscriptionId, Instant startDate, Instant endDate);
}
