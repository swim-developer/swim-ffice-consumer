package com.github.swim_developer.infrastructure.in.rest;

import com.github.swim_developer.framework.consumer.infrastructure.in.rest.AbstractFeatureResource;
import com.github.swim_developer.framework.consumer.application.port.in.SwimQueryFeaturesPort;
import jakarta.inject.Inject;
import jakarta.ws.rs.Path;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Path("/swim/v1/features")
@Tag(name = "FF-ICE Features")
public class FeatureResource extends AbstractFeatureResource {

    @Inject
    public FeatureResource(SwimQueryFeaturesPort queryFeaturesPort) {
        super(queryFeaturesPort);
    }
}
