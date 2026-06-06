package com.github.swim_developer.infrastructure.out.xml;

import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@ApplicationScoped
public class XmlEnvelopeParser {

    // TODO: Implement envelope splitting logic for your XML format
    // DNOTAM uses AIXMBasicMessage which may contain multiple members
    // ED-254 passes through single messages
    // Your service should split according to its data model
    public List<String> splitEnvelope(String rawPayload) {
        return List.of(rawPayload);
    }
}
