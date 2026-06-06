package com.github.swim_developer.infrastructure.out.client;

import com.github.swim_developer.infrastructure.in.rest.dto.SubscriptionRequest;
import com.github.swim_developer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.infrastructure.in.rest.dto.TopicDetailsResponse;
import com.github.swim_developer.framework.application.model.SubscriptionStatusUpdate;
import com.github.swim_developer.framework.infrastructure.in.rest.TopicsListResponse;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;

import java.util.List;

@Path("/swim/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface SubscriptionManagerRestClient {

    @POST
    @Path("/subscriptions")
    @Retry(maxRetries = 3, delay = 1000, maxDuration = 120000, jitter = 500)
    @Timeout(10000)
    SubscriptionResponse createSubscription(SubscriptionRequest request);

    @GET
    @Path("/subscriptions")
    @Timeout(5000)
    List<SubscriptionResponse> getSubscriptions(
            @QueryParam("queueName") String queueName,
            @QueryParam("subscriptionStatus") String subscriptionStatus
    );

    @GET
    @Path("/subscriptions/{subscriptionId}")
    @Timeout(5000)
    SubscriptionResponse getSubscriptionDetails(@PathParam("subscriptionId") String subscriptionId);

    @PUT
    @Path("/subscriptions/{subscriptionId}")
    @Retry(maxRetries = 3, delay = 1000, maxDuration = 120000, jitter = 500)
    @Timeout(10000)
    SubscriptionResponse updateSubscriptionStatus(
            @PathParam("subscriptionId") String subscriptionId,
            SubscriptionStatusUpdate statusUpdate
    );

    @DELETE
    @Path("/subscriptions/{subscriptionId}")
    @Retry(maxRetries = 2, delay = 2000, maxDuration = 60000)
    @Timeout(10000)
    void deleteSubscription(@PathParam("subscriptionId") String subscriptionId);

    @GET
    @Path("/topics")
    @Timeout(5000)
    TopicsListResponse getTopics();

    @GET
    @Path("/topics/{topicId}")
    @Timeout(5000)
    TopicDetailsResponse getTopicDetails(@PathParam("topicId") String topicId);

    @GET
    @Path("/features")
    @Produces(MediaType.APPLICATION_XML)
    @Timeout(15000)
    String getFeatures(
            @QueryParam("typeName") String typeName,
            @QueryParam("filter") String filter,
            @QueryParam("validTime") String validTime
    );

    @PUT
    @Path("/subscriptions/{subscriptionId}/renew")
    @Retry(maxRetries = 3, delay = 1000, maxDuration = 120000, jitter = 500)
    @Timeout(10000)
    SubscriptionResponse renewSubscription(@PathParam("subscriptionId") String subscriptionId);
}
