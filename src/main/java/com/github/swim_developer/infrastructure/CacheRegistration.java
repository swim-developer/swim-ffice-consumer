package com.github.swim_developer.infrastructure;

import io.quarkus.cache.CacheName;
import io.quarkus.cache.Cache;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CacheRegistration {

    @CacheName("processed-messages")
    Cache processedMessagesCache;
}
