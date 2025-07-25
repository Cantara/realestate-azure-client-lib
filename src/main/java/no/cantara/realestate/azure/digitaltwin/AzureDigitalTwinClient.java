package no.cantara.realestate.azure.digitaltwin;

import com.azure.core.exception.HttpResponseException;
import com.azure.digitaltwins.core.BasicDigitalTwin;
import com.azure.digitaltwins.core.DigitalTwinsClient;
import com.azure.digitaltwins.core.DigitalTwinsClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import no.cantara.config.ApplicationProperties;
import no.cantara.realestate.ApiUnavailableException;
import no.cantara.realestate.ExceptionStatusType;
import no.cantara.realestate.RealEstateException;
import no.cantara.realestate.security.NotAuthorizedException;
import no.cantara.realestate.security.UnauthenticatedException;
import org.slf4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class AzureDigitalTwinClient {
    private static final Logger log = getLogger(AzureDigitalTwinClient.class);
    private final DigitalTwinsClient client;
    private String applicationId;

    public AzureDigitalTwinClient(ApplicationProperties config) {
        this.applicationId = config.get("azure.digitaltwin.appId");
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
        this.applicationId = applicationId;
        if (applicationId == null || applicationKey == null || tenantId == null || endpoint == null) {
            throw new IllegalArgumentException("Missing required configuration: \n" +
                    "azure.digitaltwin.appId, azure.digitaltwin.appSecret, azure.digitaltwin.tenantId, azure.digitaltwin.endpoint");
        }
        this.client = createClient(applicationId, applicationKey, tenantId, endpoint);
    }

    protected AzureDigitalTwinClient(DigitalTwinsClient client) {
        this.client = client;
        this.applicationId = "Hidden in native DigitalTwinsClient";
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

    public List<BasicDigitalTwin> queryForTwins(String query) throws NotAuthorizedException, UnauthenticatedException {
        try {
            return client.query(query, BasicDigitalTwin.class)
                    .stream()
                    .collect(Collectors.toList());
        } catch (HttpResponseException e) {
            switch (e.getResponse().getStatusCode()) {
                case 401 -> {
                    log.trace("Unauthenticated access to Azure Digital Twins. You need to log in first.", e);
                    throw new UnauthenticatedException("Unauthenticated access to Azure Digital Twins. You need to log in first.");
                }
                case 403 -> {
                    log.trace("Not authorized to access Azure Digital Twins. applicationId: {}", applicationId, e);
                    throw new NotAuthorizedException("Not authorized to access Azure Digital Twins. applicationId: " + applicationId);
                }
                case 500, 502, 503, 504 -> {
                    log.warn("Azure Digital Twins service is currently unavailable. Please try again later.", e);
                    throw  new ApiUnavailableException("Azure Digital Twins service is currently unavailable. Please try again later.",e, ExceptionStatusType.RETRY_MAY_FIX_ISSUE);

                }
                default -> {
                    log.error("Unexpected error while querying Azure Digital Twins: {}", e.getMessage(), e);
                    throw new RealEstateException("Unexpected error while querying Azure Digital Twins: " + e.getMessage(), e);
                }
            }
        }
    }

}