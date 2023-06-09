package no.cantara.realestate.azure.rec;

import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RecObservationMessageTest {

    ObservationMessage observationMessage;
    @BeforeEach
    void setUp() {
        observationMessage = buildStubObservation();
    }

    @Test
    void quantityKind() {
        RecObservationMessage recObservation = new RecObservationMessage(observationMessage);
        assertNotNull(recObservation.getQuantityKind());
        assertEquals("https://w3id.org/rec/core/Temperature", recObservation.getQuantityKind());
        recObservation.setSensorType("co2");
        assertEquals("https://w3id.org/rec/core/CO2", recObservation.getQuantityKind());
        recObservation.setSensorType("energy");
        assertEquals("", recObservation.getQuantityKind());

    }

    private static ObservationMessage buildStubObservation() {
        Instant observedAt = Instant.now().minusSeconds(10);
        Instant receivedAt = Instant.now();
        ObservationMessage observationMessage = new ObservationMessageBuilder()
                .withSensorId("rec1")
                .withRealEstate("RE1")
                .withBuilding("Building1")
                .withFloor("04")
                .withSection("Section West")
                .withServesRoom("Room1")
                .withPlacementRoom("Room21")
                .withClimateZone("air1")
                .withElectricityZone("light")
                .withSensorType("temp")
                .withMeasurementUnit("C")
                .withValue(22)
                .withObservationTime(observedAt)
                .withReceivedAt(receivedAt)
                .withTfm("TFM12345")
                .build();
        return observationMessage;
    }
}