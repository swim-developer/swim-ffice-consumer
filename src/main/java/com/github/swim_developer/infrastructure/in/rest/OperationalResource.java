package com.github.swim_developer.infrastructure.in.rest;

import com.github.swim_developer.framework.consumer.infrastructure.in.rest.AbstractOperationalResource;
import com.github.swim_developer.framework.consumer.application.port.out.DeadLetterStore;
import com.github.swim_developer.framework.consumer.application.port.in.ConsumerStatisticsPort;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/swim/v1/operational")
@Tag(name = "FF-ICE Operational")
public class OperationalResource extends AbstractOperationalResource {

    @Inject
    public OperationalResource(DeadLetterStore dlqRepository, ConsumerStatisticsPort statisticsPort) {
        super(dlqRepository, statisticsPort);
    }
}
