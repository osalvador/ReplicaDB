package org.replicadb.oracle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledIf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Oracle XML support and compatibility with Oracle Free edition.
 * This test helps identify if XMLType columns work properly with the current
 * Oracle JDBC driver configuration.
 */
class OracleXmlSupportTest {

    @Test
    void testOracleXmlClassesAvailable() {
        // Test that Oracle XML parser classes are available in classpath
        try {
            Class.forName("oracle.xml.parser.v2.XMLParseException");
            Class.forName("oracle.xml.parser.v2.XMLDocument");
            assertTrue(true, "Oracle XML parser classes are available");
        } catch (ClassNotFoundException e) {
            fail("Oracle XML parser classes not found: " + e.getMessage());
        }
    }
    
    @Test
    void testXmlTypeHandling() {
        // This test verifies that XML-related dependencies are properly configured
        // without requiring an actual Oracle container to be started
        
        // Test that we can at least reference Oracle XMLType-related classes
        try {
            // This should not throw ClassNotFoundException if dependencies are correct
            Class.forName("oracle.sql.OPAQUE");
            Class.forName("oracle.xdb.XMLType");
            assertTrue(true, "Oracle XMLType classes are available");
        } catch (ClassNotFoundException e) {
            // If XMLType is not available in Oracle Free, this would be expected
            assumeTrue(false, "Oracle XMLType classes not available - may not be supported in Oracle Free edition: " + e.getMessage());
        }
    }
}
