package com.github.swim_developer.domain.model.command;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@RegisterForReflection
public record SubscriptionCommand(
        String topic,
        String queueName,
        String provider,
        String description,
        List<String> messageTypes,
        List<String> aerodromes
) {
    public String generateConfigHash() {
        String prov = provider != null ? provider : "";
        String t = topic != null ? topic : "";
        return sha256(prov + t);
    }

    private static String sha256(String content) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256")
                            .digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
