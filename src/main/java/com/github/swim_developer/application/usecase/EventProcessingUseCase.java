package com.github.swim_developer.application.usecase;

import com.github.swim_developer.application.service.ProcessingMetrics;
import com.github.swim_developer.application.port.out.SubscriptionStore;
import com.github.swim_developer.application.service.EventDataValidator;
import com.github.swim_developer.application.service.EventFilterService;
import com.github.swim_developer.application.service.EventPersistenceService;
import com.github.swim_developer.application.service.ProcessorCallbacks;
import com.github.swim_developer.domain.model.Event;
import aero.fixm.ffice.FficeMessageType;
import com.github.swim_developer.infrastructure.out.xml.EventExtractor;
import com.github.swim_developer.framework.consumer.application.messaging.processing.DefaultEventProcessorConfig;
import com.github.swim_developer.framework.application.model.PreparedEvent;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.application.model.ProcessingOutcome;
import com.github.swim_developer.framework.consumer.application.messaging.processing.EventProcessingOrchestrator;
import com.github.swim_developer.framework.consumer.application.messaging.processing.EventProcessingOrchestratorDependencies;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventParser;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventProcessorCallbacks;
import com.github.swim_developer.framework.application.port.in.SwimMessageInterceptor;
import com.github.swim_developer.framework.application.port.out.SwimXmlUnmarshallerPort;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class EventProcessingUseCase {

    private final EventProcessingOrchestrator<Event, FficeMessageType> orchestrator;
    private final EventPersistenceService persistenceService;

    @Inject
    public EventProcessingUseCase(
            DefaultEventProcessorConfig processorConfig,
            SwimXmlUnmarshallerPort<FficeMessageType> jaxbPool,
            EventExtractor eventExtractor,
            EventDataValidator validator,
            EventFilterService filterService,
            EventPersistenceService persistenceService,
            ProcessingMetrics metrics,
            MeterRegistry meterRegistry,
            SubscriptionStore subscriptionStore,
            @Any Instance<SwimMessageInterceptor> interceptorInstances) {
        this.persistenceService = persistenceService;
        SwimEventParser<FficeMessageType> parser = jaxbPool::unmarshalAndValidate;
        SwimEventProcessorCallbacks<Event> callbacks = new ProcessorCallbacks(metrics, subscriptionStore);
        this.orchestrator = new EventProcessingOrchestrator<>(new EventProcessingOrchestratorDependencies<>(
                processorConfig, parser, eventExtractor, validator, filterService,
                persistenceService, callbacks, meterRegistry, interceptorInstances));
    }

    public ProcessingOutcome processAndPersistSingleMessage(String subscriptionId, String queueName,
                                                            String amqpMessageId, String xml, int index) {
        return orchestrator.processMessage(new ProcessingContext(subscriptionId, queueName, amqpMessageId, xml, index, null));
    }

    public EventProcessingOrchestrator<Event, FficeMessageType> eventProcessingOrchestrator() {
        return orchestrator;
    }

    public void batchPersistAndDispatch(List<PreparedEvent<Event>> batch) {
        persistenceService.batchPersistAndDispatch(batch);
    }

    public void markBatchAsProcessed(List<PreparedEvent<Event>> batch) {
        orchestrator.markBatchAsProcessed(batch);
    }
}
