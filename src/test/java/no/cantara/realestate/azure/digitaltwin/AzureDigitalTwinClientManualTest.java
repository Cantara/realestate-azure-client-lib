package no.cantara.realestate.azure.digitaltwin;

import com.azure.digitaltwins.core.BasicDigitalTwin;
import no.cantara.config.ApplicationProperties;

import java.util.List;

class AzureDigitalTwinClientManualTest {

    public static void main(String[] args) {
        ApplicationProperties config = ApplicationProperties.builder().defaults().buildAndSetStaticSingleton();
        AzureDigitalTwinClient client = new AzureDigitalTwinClient(config);
        List<BasicDigitalTwin> twins = client.queryForTwins("SELECT T.$dtId, T.identifiers.metasys_id FROM digitaltwins T WHERE contains(T.customProperties.source.System, 'Metasys')");
        for (BasicDigitalTwin twin : twins) {
            System.out.println("twin = " + twin);
        }
    }
}