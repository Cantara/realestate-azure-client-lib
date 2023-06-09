package no.cantara.realestate.azure.rec;

import com.fasterxml.jackson.core.JsonProcessingException;
import no.cantara.realestate.json.RealEstateObjectMapper;
import no.cantara.realestate.observations.ObservationMessage;
import no.cantara.realestate.observations.ObservationMessageBuilder;
import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class RecObservationJsonTest {

    RecObservationMessage recObservationMessage;
    private RealEstateObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        ObservationMessage observationMessage = buildStubObservation();
        recObservationMessage = new RecObservationMessage(observationMessage);
        objectMapper = RealEstateObjectMapper.getInstance();
    }

    @Test
    void validateRecJsonFormat() throws JsonProcessingException, JSONException {
        //FIXME add "observationTime": "2019-05-27T20:07:44Z  "
        String observationJson = objectMapper.getObjectMapper().writeValueAsString(recObservationMessage);
        assertNotNull(observationJson);
        System.out.println(observationJson);
        String expectedJson = """
                {
                  "sensorId": "rec1",
                  "tfm": "TFM12345",
                  "realEstate": "RE1",
                  "building": "Building1",
                  "floor": "04",
                  "section": "Section West",
                  "servesRoom": "Room1",
                  "placementRoom": "Room21",
                  "climateZone": "air1",
                  "electricityZone": "light",
                  "name": null,
                  "sensorType": "temp",
                  "measurementUnit": "C",
                  "value": 22,
                  "observationTime": null,
                  "receivedAt": null,
                  "quantityKind": "https://w3id.org/rec/core/Temperature"
                }
                """;
        JSONAssert.assertEquals(expectedJson, observationJson,false);
    }

    public static ObservationMessage buildStubObservation() {
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
                .withTfm("TFM12345")
                .build();
        return observationMessage;
    }
}
