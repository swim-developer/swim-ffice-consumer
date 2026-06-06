package com.github.swim_developer.infrastructure.out.xml;

import aero.fixm.base.AerodromeReferenceType;
import aero.fixm.base.GloballyUniqueFlightIdentifierType;
import aero.fixm.ffice.FficeMessageType;
import aero.fixm.ffice.MessageTypeType;
import aero.fixm.flight.ArrivalType;
import aero.fixm.flight.DepartureType;
import aero.fixm.flight.FlightIdentificationType;
import aero.fixm.flight.FlightType;
import com.github.swim_developer.domain.model.Event;
import com.github.swim_developer.framework.application.port.out.SwimEventExtractor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.xml.bind.JAXBElement;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
@ApplicationScoped
public class EventExtractor implements SwimEventExtractor<Event, FficeMessageType> {

    @Override
    public String getTypeLabel(Event event) {
        return event.getFficeMessageType() != null ? event.getFficeMessageType() : "unknown";
    }

    @Override
    public List<Optional<Event>> extract(FficeMessageType fficeMessage) {
        if (fficeMessage == null) {
            return List.of(Optional.empty());
        }

        Event event = new Event();

        MessageTypeType msgType = fficeMessage.getType();
        if (msgType != null) {
            event.setFficeMessageType(msgType.name());
        }

        if (fficeMessage.getTimestamp() != null) {
            event.setMessageTimestamp(fficeMessage.getTimestamp().toString());
        }
        if (fficeMessage.getUniqueMessageIdentifier() != null) {
            event.setUniqueMessageIdentifier(fficeMessage.getUniqueMessageIdentifier().getValue());
        }

        FlightType flight = fficeMessage.getFlight();
        if (flight != null) {
            extractFlightData(event, flight);
        }

        return List.of(Optional.of(event));
    }

    private void extractFlightData(Event event, FlightType flight) {
        FlightIdentificationType flightId = unwrap(flight.getFlightIdentification());
        if (flightId != null) {
            GloballyUniqueFlightIdentifierType gufi = unwrap(flightId.getGufi());
            if (gufi != null) {
                event.setGufi(gufi.getValue());
            }

            String acId = unwrap(flightId.getAircraftIdentification());
            if (acId != null) {
                event.setAircraftIdentification(acId);
            }
        }

        DepartureType departure = unwrap(flight.getDeparture());
        if (departure != null) {
            event.setDepartureAerodrome(extractLocationIndicator(departure.getDepartureAerodrome()));
        }

        ArrivalType arrival = unwrap(flight.getArrival());
        if (arrival != null) {
            String dest = extractLocationIndicator(arrival.getDestinationAerodrome());
            if (dest == null) {
                dest = extractLocationIndicator(arrival.getArrivalAerodrome());
            }
            event.setArrivalAerodrome(dest);
        }
    }

    private String extractLocationIndicator(JAXBElement<AerodromeReferenceType> element) {
        AerodromeReferenceType ref = unwrap(element);
        if (ref == null) {
            return null;
        }
        return unwrap(ref.getLocationIndicator());
    }

    private static <T> T unwrap(JAXBElement<T> element) {
        return (element != null && element.getValue() != null) ? element.getValue() : null;
    }
}
