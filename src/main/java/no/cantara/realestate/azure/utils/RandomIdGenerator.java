package no.cantara.realestate.azure.utils;

import java.security.SecureRandom;

public class RandomIdGenerator {
    private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz";
    private static final int LENGTH = 5;
    private static final SecureRandom RANDOM = new SecureRandom();

    public static String generateRandomId() {
        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            int index = RANDOM.nextInt(CHARACTERS.length());
            sb.append(CHARACTERS.charAt(index));
        }
        return sb.toString();
    }
}
