package no.cantara.realestate.azure.storage;

import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzureTableClientTest {

    @Test
    void listRows() {
        TableClient tableClient = mock(TableClient.class);
        AzureTableClient azureTableClient = new AzureTableClient(tableClient);
        ArrayList<TableEntity> stubRows = new ArrayList<>();
        TableEntity tableEntitiy = new TableEntity("1", "1");
        tableEntitiy.addProperty("RealEstate", "1");
        stubRows.add(tableEntitiy);
        PagedIterable<TableEntity> pagedIterable = mock(PagedIterable.class);
        when(pagedIterable.stream()).thenReturn(stubRows.stream());
        when(tableClient.listEntities(isA(ListEntitiesOptions.class), isNull(), isNull())).thenReturn(pagedIterable);
        assertEquals(1, azureTableClient.listRows("1").size());
    }
}