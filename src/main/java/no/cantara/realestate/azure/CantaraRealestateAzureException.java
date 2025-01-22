package no.cantara.realestate.azure;

import java.security.SecureRandom;

import static no.cantara.realestate.azure.utils.RandomIdGenerator.generateRandomId;

public class CantaraRealestateAzureException extends Exception {
    private static final int LENGTH = 5;
    private static final SecureRandom RANDOM = new SecureRandom();

    public CantaraRealestateAzureException(String message) {
        super(message + " :MessageId: CRAE-" + generateRandomId());
    }
    public CantaraRealestateAzureException(String message, Exception e) {
        super(message + " :MessageId: CRAE-" + generateRandomId(), e);
    }

}
