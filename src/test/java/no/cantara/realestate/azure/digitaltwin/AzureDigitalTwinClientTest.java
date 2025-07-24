package no.cantara.realestate.azure.digitaltwin;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.digitaltwins.core.BasicDigitalTwin;
import com.azure.digitaltwins.core.DigitalTwinsClient;
import no.cantara.realestate.azure.RealestateNotAuthorized;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class AzureDigitalTwinClientTest {

    @Mock
    private DigitalTwinsClient mockClient;

    @Mock
    private HttpResponse mockResponse;

    @Mock
    private HttpRequest mockRequest;

    private AzureDigitalTwinClient azureDigitalTwinClient;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        azureDigitalTwinClient = new AzureDigitalTwinClient(mockClient);
    }

    @Test
    void queryForTwins_shouldThrowRealestateNotAuthorized_when403StatusCode() {
        // Arrange
        String query = "SELECT * FROM digitaltwins";
        when(mockResponse.getStatusCode()).thenReturn(403);
        when(mockRequest.getUrl()).thenReturn(null);
        // Simulate the exact exception from the stack trace: Status code 403, (empty body)
        HttpResponseException httpResponseException = new HttpResponseException("Status code 403, (empty body)", mockResponse, mockRequest);
        
        when(mockClient.query(eq(query), eq(BasicDigitalTwin.class)))
                .thenThrow(httpResponseException);

        // Act & Assert
        RealestateNotAuthorized exception = assertThrows(RealestateNotAuthorized.class, 
                () -> azureDigitalTwinClient.queryForTwins(query));
        
        assertEquals("Not authorized to access Azure Digital Twins. Please check your credentials and permissions.", 
                exception.getMessage().split(" :MessageId:")[0]);
        assertEquals(httpResponseException, exception.getCause());
    }

    @Test
    void queryForTwins_shouldRethrowOtherHttpResponseExceptions_whenNot403StatusCode() {
        // Arrange
        String query = "SELECT * FROM digitaltwins";
        when(mockResponse.getStatusCode()).thenReturn(500);
        when(mockRequest.getUrl()).thenReturn(null);
        HttpResponseException httpResponseException = new HttpResponseException("Internal Server Error", mockResponse, mockRequest);
        
        when(mockClient.query(eq(query), eq(BasicDigitalTwin.class)))
                .thenThrow(httpResponseException);

        // Act & Assert
        HttpResponseException exception = assertThrows(HttpResponseException.class, 
                () -> azureDigitalTwinClient.queryForTwins(query));
        
        assertEquals(httpResponseException, exception);
    }
}