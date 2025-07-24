package no.cantara.realestate.azure.digitaltwin;

import com.azure.core.exception.HttpResponseException;
import com.azure.digitaltwins.core.BasicDigitalTwin;
import com.azure.digitaltwins.core.DigitalTwinsClient;
import com.azure.digitaltwins.core.DigitalTwinsClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.azure.RealestateNotAuthorized;

import java.util.List;
import java.util.stream.Collectors;

public class AzureDigitalTwinClient {

    private final DigitalTwinsClient client;

    public AzureDigitalTwinClient(ApplicationProperties config) {
        String applicationId = config.get("azure.digitaltwin.appId");
        String applicationKey = config.get("azure.digitaltwin.appSecret");
        String tenantId = config.get("azure.digitaltwin.tenantId");
        String endpoint = config.get("azure.digitaltwin.endpoint");
        if (applicationId == null || applicationKey == null || tenantId == null || endpoint == null) {
            throw new IllegalArgumentException("Missing required configuration: \n" +
                    "azure.digitaltwin.appId, azure.digitaltwin.appSecret, azure.digitaltwin.tenantId, azure.digitaltwin.endpoint");
        }

        this.client = createClient(applicationId, applicationKey, tenantId, endpoint);
    }

    public AzureDigitalTwinClient(String applicationId, String applicationKey, String tenantId, String endpoint) {
        if (applicationId == null || applicationKey == null || tenantId == null || endpoint == null) {
            throw new IllegalArgumentException("Missing required configuration: \n" +
                    "azure.digitaltwin.appId, azure.digitaltwin.appSecret, azure.digitaltwin.tenantId, azure.digitaltwin.endpoint");
        }
        this.client = createClient(applicationId, applicationKey, tenantId, endpoint);
    }

    protected AzureDigitalTwinClient(DigitalTwinsClient client) {
        this.client = client;
    }

    protected DigitalTwinsClient createClient(String applicationId, String applicationKey, String tenantId, String endpoint) {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(applicationId)
                .clientSecret(applicationKey)
                .tenantId(tenantId)
                .build();

        return new DigitalTwinsClientBuilder()
                .credential(clientSecretCredential)
                .endpoint(endpoint)
                .buildClient();
    }

    public List<BasicDigitalTwin> queryForTwins(String query) throws RealestateNotAuthorized {
        try {
            return client.query(query, BasicDigitalTwin.class)
                    .stream()
                    .collect(Collectors.toList());
        } catch (HttpResponseException e) {
            if (e.getResponse().getStatusCode() == 403) {
                throw new RealestateNotAuthorized("Not authorized to access Azure Digital Twins. Please check your credentials and permissions.", e);
            }
            throw e;
        }
    }


}