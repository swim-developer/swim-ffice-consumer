package com.github.swim_developer.infrastructure.in.rest;

import com.github.swim_developer.domain.model.Subscription;
import com.github.swim_developer.domain.model.command.SubscriptionCommand;
import com.github.swim_developer.application.port.in.ManageSubscriptionPort;
import com.github.swim_developer.application.port.out.SubscriptionStore;
import com.github.swim_developer.infrastructure.in.rest.dto.SubscriptionRequest;
import com.github.swim_developer.framework.consumer.infrastructure.out.config.subscription.AbstractSubscriptionConfigParser;
import static com.github.swim_developer.framework.consumer.infrastructure.in.rest.ConsumerRestResponses.*;
import com.github.swim_developer.framework.infrastructure.util.StringUtil;
import com.github.swim_developer.framework.application.model.SubscriptionStatusUpdate;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import java.util.Map;

@Slf4j
@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "SWIM FF-ICE Consumer API", description = "Subscription management endpoints")
public class ConsumerSubscriptionResource {

    private final SubscriptionStore subscriptionRepository;
    private final AbstractSubscriptionConfigParser<?> configParser;
    private final ManageSubscriptionPort subscriptionService;

    @Inject
    public ConsumerSubscriptionResource(SubscriptionStore subscriptionRepository,
                                        AbstractSubscriptionConfigParser<?> configParser,
                                        ManageSubscriptionPort subscriptionService) {
        this.subscriptionRepository = subscriptionRepository;
        this.configParser = configParser;
        this.subscriptionService = subscriptionService;
    }

    @GET
    @Path("/subscriptions")
    @Operation(summary = "List all subscriptions")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "List of subscriptions",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON,
                            schema = @Schema(implementation = Subscription.class, type = SchemaType.ARRAY)))
    })
    public Response listAllSubscriptions() {
        return Response.ok(subscriptionRepository.findAllSubscriptions()).build();
    }

    @GET
    @Path("/subscriptions/active")
    @Operation(summary = "List active subscriptions")
    public Response listActiveSubscriptions() {
        return Response.ok(subscriptionRepository.findActiveSubscriptions()).build();
    }

    @POST
    @Path("/subscriptions")
    @Operation(summary = "Create subscription")
    @APIResponses(value = {
            @APIResponse(responseCode = "201", description = "Subscription created"),
            @APIResponse(responseCode = "400", description = "Invalid request"),
            @APIResponse(responseCode = "503", description = "Subscription Manager unavailable")
    })
    public Response createSubscription(
            @RequestBody(description = "Subscription details", required = true,
                    content = @Content(schema = @Schema(implementation = SubscriptionRequest.class)))
            SubscriptionRequest request) {

        if (StringUtil.isNullOrBlank(request.topic())) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of(ERROR_KEY, "topic is required"))
                    .build();
        }

        try {
            SubscriptionCommand command = toCommand(request);
            Subscription subscription = subscriptionService.createSubscription(command);
            log.info("Created subscription: {}", subscription.getSubscriptionId());
            return created(subscription);
        } catch (Exception e) {
            log.error("Failed to create subscription", e);
            return serviceUnavailable("Failed to create subscription: " + e.getMessage());
        }
    }

    @DELETE
    @Path("/subscriptions/{subscriptionId}")
    @Operation(summary = "Delete subscription")
    public Response deleteSubscription(
            @Parameter(description = "Subscription ID", required = true)
            @PathParam("subscriptionId") String subscriptionId) {
        try {
            subscriptionService.deleteSubscriptionById(subscriptionId);
            return noContent();
        } catch (IllegalArgumentException e) {
            return notFound(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to delete subscription: {}", subscriptionId, e);
            return serviceUnavailable("Failed to delete subscription: " + e.getMessage());
        }
    }

    @PUT
    @Path("/subscriptions/{subscriptionId}")
    @Operation(summary = "Update subscription status")
    public Response updateSubscriptionStatus(
            @PathParam("subscriptionId") String subscriptionId,
            SubscriptionStatusUpdate statusUpdate) {

        if (statusUpdate == null || statusUpdate.subscriptionStatus() == null) {
            return badRequest("subscriptionStatus is required");
        }

        String newStatus = statusUpdate.subscriptionStatus().toUpperCase();
        if (!isValidSubscriptionStatus(newStatus)) {
            return badRequest("subscriptionStatus must be ACTIVE, PAUSED, or DELETED");
        }

        try {
            Subscription subscription;
            if (newStatus.equals(SubscriptionStatus.DELETED.name())) {
                subscriptionService.deleteSubscriptionById(subscriptionId);
                return noContent();
            } else if (newStatus.equals(SubscriptionStatus.PAUSED.name())) {
                subscription = subscriptionService.pauseSubscription(subscriptionId);
            } else {
                subscription = subscriptionService.resumeSubscription(subscriptionId);
            }
            return ok(subscription);
        } catch (IllegalArgumentException e) {
            return notFound(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to update subscription: {}", subscriptionId, e);
            return serviceUnavailable("Failed to update subscription: " + e.getMessage());
        }
    }

    @GET
    @Path("/topics")
    @Operation(summary = "List configured topics")
    public Response listConfiguredTopics() {
        List<?> commands = configParser.parseDesiredSubscriptions();
        return Response.ok(commands).build();
    }

    // TODO: Adapt toCommand mapping for your domain-specific SubscriptionCommand fields
    private static SubscriptionCommand toCommand(SubscriptionRequest request) {
        return new SubscriptionCommand(
                request.topic(),
                request.queueName(),
                request.provider(),
                request.description(),
                List.of(),
                List.of()
        );
    }
}
