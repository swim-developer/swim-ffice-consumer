package com.github.swim_developer.domain.model;

import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.domain.model.SwimOutboxEvent;
import io.quarkus.mongodb.panache.common.MongoEntity;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.types.ObjectId;

import java.time.Instant;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@MongoEntity(collection = "ffice_events")
public class Event implements SwimOutboxEvent {

    private ObjectId id;
    private String messageId;
    private String subscriptionId;
    private String queueName;
    private String contentHash;
    private String rawPayload;
    private Instant receivedAt;
    private OutboxDeliveryStatus deliveryStatus;
    private int dispatchRetryCount;
    private Instant dispatchedAt;

    private String fficeMessageType;
    private String gufi;
    private String aircraftIdentification;
    private String departureAerodrome;
    private String arrivalAerodrome;
    private String messageTimestamp;
    private String uniqueMessageIdentifier;

    @Override
    public int getOutboxRetryCount() { return dispatchRetryCount; }

    @Override
    public void setOutboxRetryCount(int count) { this.dispatchRetryCount = count; }
}
