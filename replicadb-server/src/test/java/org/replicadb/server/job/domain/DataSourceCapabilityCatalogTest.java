package org.replicadb.server.job.domain;

import org.junit.jupiter.api.Test;
import org.replicadb.cli.ReplicationMode;
import org.replicadb.manager.DataSourceType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataSourceCapabilityCatalogTest {

    private final DataSourceCapabilityCatalog catalog = new DataSourceCapabilityCatalog();

    @Test
    void mapsWireValuesAndConnectionSchemes() {
        assertEquals(ConnectorType.POSTGRES, ConnectorType.fromWireValue("POSTGRES"));
        assertEquals(ConnectorType.MONGODBSRV,
                ConnectorType.fromConnection("mongodb+srv://cluster.example/database"));
        assertEquals(ConnectorType.SQLITE, ConnectorType.fromConnection("jdbc:sqlite:/tmp/source.db"));
        assertEquals(ConnectorType.CUSTOM, ConnectorType.fromConnection("jdbc:custom:database"));
    }

    @Test
    void delegatesRoleAndModeRulesToTheCoreCatalog() {
        DataSourceCapabilities kafka = catalog.forType(ConnectorType.KAFKA);
        assertFalse(kafka.supportsSource());
        assertTrue(kafka.supports(DataSourceType.SINK, ReplicationMode.COMPLETE));
        assertFalse(kafka.supports(DataSourceType.SINK, ReplicationMode.INCREMENTAL));

        DataSourceCapabilities denodo = catalog.forType(ConnectorType.DENODO);
        assertTrue(denodo.supportsSource());
        assertFalse(denodo.supportsSink());
    }

    @Test
    void rejectsCustomCapabilitiesUntilAValidatedSchemeExists() {
        assertThrows(IllegalArgumentException.class, () -> catalog.forType(ConnectorType.CUSTOM));
        assertThrows(IllegalArgumentException.class, () -> ConnectorType.fromWireValue("unknown"));
        assertThrows(IllegalArgumentException.class, () -> ConnectorType.fromConnection(" "));
    }
}
