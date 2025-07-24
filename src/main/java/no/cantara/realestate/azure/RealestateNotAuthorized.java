package no.cantara.realestate.azure;

/**
 * Exception thrown when the client is not authorized to access Azure resources.
 * This typically occurs when credentials are invalid or the client lacks necessary permissions.
 */
public class RealestateNotAuthorized extends CantaraRealestateAzureException {

    public RealestateNotAuthorized(String message) {
        super(message);
    }

    public RealestateNotAuthorized(String message, Exception e) {
        super(message, e);
    }
}