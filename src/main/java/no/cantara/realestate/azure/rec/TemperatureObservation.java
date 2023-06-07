package no.cantara.realestate.azure.rec;

import java.time.Instant;

import static no.cantara.realestate.azure.rec.ObservationTypes.TEMPERATURE;


public class TemperatureObservation extends Observation {

    private final double value;

    /**
     * Create an observation
     * @param sensorId id of sensor
     * @param value temperature in C
     */
    public TemperatureObservation(String sensorId,  double value) {
        super( REC_NS_SENSOR + sensorId, TEMPERATURE);
        this.value = value;
    }

    /*
    Used for testing
     */
    protected TemperatureObservation(String sensorId, double value, Instant observationTime) {
        super( REC_NS_SENSOR + sensorId, TEMPERATURE, observationTime);
        this.value = value;
    }

    @Override
    Object getValue() {
        return value;
    }
}
