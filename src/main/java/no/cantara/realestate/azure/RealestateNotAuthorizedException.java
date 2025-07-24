package no.cantara.realestate.azure;

/**
 * Exception thrown when the client is not authorized to access Azure resources.
 * This typically occurs when credentials are invalid or the client lacks necessary permissions.
 */
public class RealestateNotAuthorizedException extends CantaraRealestateAzureException {

    public RealestateNotAuthorizedException(String message) {
        super(message);
    }

    public RealestateNotAuthorizedException(String message, Exception e) {
        super(message, e);
    }
}