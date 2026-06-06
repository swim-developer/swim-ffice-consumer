package com.github.swim_developer.infrastructure.in.rest.dto;

public record TopicDetailsResponse(
        String topicId,
        String topicName,
        String description,
        String publisherState
) {}
