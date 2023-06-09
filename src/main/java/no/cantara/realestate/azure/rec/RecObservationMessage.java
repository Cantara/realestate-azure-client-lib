package no.cantara.realestate.azure.rec;

import no.cantara.realestate.observations.ObservationMessage;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RecObservationMessage extends ObservationMessage {
    private final String TEMPERATURE = "temperatur";
    private final String CO2 = "co2";
    private final String PRESCENCE = "tilstedevarelse";
    private String quantityKind;

    public RecObservationMessage() {
    }

    public RecObservationMessage(ObservationMessage observationMessage) {
        for (Method getMethod : observationMessage.getClass().getMethods()) {
            if (getMethod.getName().startsWith("get")) {
                try {
                    Method setMethod = this.getClass().getMethod(getMethod.getName().replace("get", "set"), getMethod.getReturnType());
                    setMethod.invoke(this, getMethod.invoke(observationMessage, (Object[]) null));

                } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException |
                         InvocationTargetException ex) {
                    //not found set
                }
            }
        }
        addQuantityKind(observationMessage.getSensorType());
    }


    public void addQuantityKind(String sensorType) {
        if (sensorType != null) {
            switch (sensorType.toLowerCase()) {
                case TEMPERATURE:
                case "temp":
                    quantityKind = "https://w3id.org/rec/core/Temperature";
                    break;
                case CO2:
                    quantityKind = "https://w3id.org/rec/core/CO2";
                    break;
                case PRESCENCE:
                    quantityKind = "https://w3id.org/rec/core/Prescence";
                    break;
                default:
                    quantityKind = "";
            }
        }
    }

    public String getQuantityKind() {
        return quantityKind;
    }

    public void setQuantityKind(String quantityKind) {
        this.quantityKind = quantityKind;
    }

    @Override
    public void setSensorType(String sensorType) {
        super.setSensorType(sensorType);
        addQuantityKind(sensorType);
    }
}
