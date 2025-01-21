package no.cantara.realestate.azure.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;

public class ManualAppClientLogin {
    public static void main(String[] args) {
        String tenantId = args[0];
        String clientId = args[1];
        String clientSecret = args[2];
        TokenCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();
        // Hent JWT token
        AccessToken token = clientSecretCredential.getToken(new TokenRequestContext().addScopes("https://management.azure.com/.default")).block();

        // Skriv ut JWT token
        System.out.println("JWT Token: " + token.getToken());
    }
}
