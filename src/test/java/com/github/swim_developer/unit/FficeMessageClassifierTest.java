package com.github.swim_developer.unit;

import com.github.swim_developer.extension.outbox.kafka.ffice.FficeEventCategory;
import com.github.swim_developer.extension.outbox.kafka.ffice.FficeMessageClassifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class FficeMessageClassifierTest {

    @ParameterizedTest
    @CsvSource({
            "events/filed-flight-plan.xml, FLIGHT_PLAN",
            "events/flight-plan-update.xml, FLIGHT_UPDATE",
            "events/flight-departure.xml, OPERATIONS",
            "events/flight-arrival.xml, OPERATIONS",
            "events/flight-cancellation.xml, OPERATIONS",
            "events/planning-status.xml, FLIGHT_UPDATE",
            "events/filing-status.xml, FLIGHT_UPDATE"
    })
    void classifyRealXmlSamples(String resourcePath, String expectedCategory) throws IOException {
        String xml = loadResource(resourcePath);

        FficeEventCategory category = FficeMessageClassifier.classify(xml);

        assertThat(category).isEqualTo(FficeEventCategory.valueOf(expectedCategory));
    }

    @Test
    void classifyUnknownMessage() {
        String xml = "<ffice:FficeMessage><ffice:type>SOMETHING_NEW</ffice:type></ffice:FficeMessage>";

        assertThat(FficeMessageClassifier.classify(xml)).isEqualTo(FficeEventCategory.UNKNOWN);
    }

    @Test
    void extractGufiFromInlineElement() {
        String xml = "<FficeMessage><globallyUniqueFlightIdentifier>abc-123</globallyUniqueFlightIdentifier></FficeMessage>";

        String gufi = FficeMessageClassifier.extractGufi(xml);

        assertThat(gufi).isEqualTo("abc-123");
    }

    @Test
    void extractGufiFromNamespacedElement() {
        String xml = "<ffice:FficeMessage><fx:gufi codeSpace=\"urn:uuid\">f47ac10b-58cc</fx:gufi></ffice:FficeMessage>";

        assertThat(FficeMessageClassifier.extractGufi(xml)).isEqualTo("unknown");
    }

    @Test
    void extractGufiReturnsUnknownWhenMissing() {
        String xml = "<ffice:FficeMessage><ffice:type>FILED_FLIGHT_PLAN</ffice:type></ffice:FficeMessage>";

        assertThat(FficeMessageClassifier.extractGufi(xml)).isEqualTo("unknown");
    }

    private static String loadResource(String path) throws IOException {
        try (InputStream is = FficeMessageClassifierTest.class.getClassLoader().getResourceAsStream(path)) {
            assertThat(is).as("Resource %s not found", path).isNotNull();
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
