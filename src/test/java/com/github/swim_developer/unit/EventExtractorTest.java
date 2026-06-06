package com.github.swim_developer.unit;

import aero.fixm.ffice.FficeMessageType;
import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.infrastructure.out.xml.EventExtractor;
import com.github.swim_developer.infrastructure.out.xml.JaxbUnmarshallerPool;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class EventExtractorTest {

    private static final JaxbUnmarshallerPool unmarshallerPool = new JaxbUnmarshallerPool();
    private final EventExtractor extractor = new EventExtractor();

    private static String filedFlightPlanXml;
    private static String flightDepartureXml;
    private static String flightArrivalXml;

    @BeforeAll
    static void loadXmlSamples() throws IOException {
        filedFlightPlanXml = loadResource("events/filed-flight-plan.xml");
        flightDepartureXml = loadResource("events/flight-departure.xml");
        flightArrivalXml = loadResource("events/flight-arrival.xml");
    }

    @Test
    void extractFiledFlightPlan() throws Exception {
        FficeMessageType msg = unmarshallerPool.unmarshalAndValidate(filedFlightPlanXml);

        List<Optional<Event>> results = extractor.extract(msg);

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isPresent();

        Event event = results.get(0).get();
        assertThat(event.getFficeMessageType()).isEqualTo("FILED_FLIGHT_PLAN");
        assertThat(event.getAircraftIdentification()).isEqualTo("TAP123");
        assertThat(event.getDepartureAerodrome()).isEqualTo("LPPT");
        assertThat(event.getArrivalAerodrome()).isEqualTo("LFPG");
        assertThat(event.getGufi()).isNotBlank();
        assertThat(event.getUniqueMessageIdentifier()).isNotBlank();
        assertThat(event.getMessageTimestamp()).isNotBlank();
    }

    @Test
    void extractFlightDeparture() throws Exception {
        FficeMessageType msg = unmarshallerPool.unmarshalAndValidate(flightDepartureXml);

        List<Optional<Event>> results = extractor.extract(msg);

        assertThat(results).hasSize(1);
        Event event = results.get(0).orElseThrow();
        assertThat(event.getFficeMessageType()).isEqualTo("FLIGHT_DEPARTURE");
        assertThat(event.getAircraftIdentification()).isEqualTo("IBE789");
        assertThat(event.getDepartureAerodrome()).isEqualTo("LEMD");
        assertThat(event.getArrivalAerodrome()).isEqualTo("EDDF");
    }

    @Test
    void extractFlightArrival() throws Exception {
        FficeMessageType msg = unmarshallerPool.unmarshalAndValidate(flightArrivalXml);

        List<Optional<Event>> results = extractor.extract(msg);

        assertThat(results).hasSize(1);
        Event event = results.get(0).orElseThrow();
        assertThat(event.getFficeMessageType()).isEqualTo("FLIGHT_ARRIVAL");
        assertThat(event.getAircraftIdentification()).isEqualTo("AFR101");
        assertThat(event.getDepartureAerodrome()).isEqualTo("LFPG");
        assertThat(event.getArrivalAerodrome()).isEqualTo("LIRF");
    }

    @Test
    void extractNullReturnsEmpty() {
        List<Optional<Event>> results = extractor.extract(null);

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEmpty();
    }

    @Test
    void getTypeLabelReturnsMessageType() {
        Event event = new Event();
        event.setFficeMessageType("FILED_FLIGHT_PLAN");

        assertThat(extractor.getTypeLabel(event)).isEqualTo("FILED_FLIGHT_PLAN");
    }

    @Test
    void getTypeLabelReturnsUnknownWhenNull() {
        Event event = new Event();

        assertThat(extractor.getTypeLabel(event)).isEqualTo("unknown");
    }

    private static String loadResource(String path) throws IOException {
        try (InputStream is = EventExtractorTest.class.getClassLoader().getResourceAsStream(path)) {
            assertThat(is).as("Resource %s not found", path).isNotNull();
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
