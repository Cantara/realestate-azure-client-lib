package no.cantara.realestate.azure.digitaltwin;

import com.azure.digitaltwins.core.BasicDigitalTwin;
import no.cantara.config.ApplicationProperties;

import java.util.List;

class AzureDigitalTwinClientManualTest {

    public static void main(String[] args) {
        ApplicationProperties config = ApplicationProperties.builder().defaults().buildAndSetStaticSingleton();
        AzureDigitalTwinClient client = new AzureDigitalTwinClient(config);
        String sourceSystem = "Desigo";
        String query = "SELECT * FROM digitaltwins T WHERE is_of_model ('dtmi:org:brickschema:schema:Brick:Sensor;1') and contains(customProperties.source.System,'%s')".formatted(sourceSystem);
        List<BasicDigitalTwin> twins = client.queryForTwins(query);
        System.out.println("Found " + twins.size() + " twins with source system: " + sourceSystem);
        for (BasicDigitalTwin twin : twins) {
            System.out.println("twin = " + twin.getId());
        }
    }
}