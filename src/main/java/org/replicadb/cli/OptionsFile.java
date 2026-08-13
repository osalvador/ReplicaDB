package org.replicadb.cli;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.replicadb.config.CredentialRedactor;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OptionsFile {

    private static final Logger LOG = LogManager.getLogger(OptionsFile.class.getName());
    private static final String SOURCE_CONNECTION_PREFIX = "source.connect.parameter.";
    private static final String SINK_CONNECTION_PREFIX = "sink.connect.parameter.";
    private static final String REPLICATION_TABLE_PREFIX = "replication.table.";
    private static final Pattern REPLICATION_TABLE_PROPERTY = Pattern.compile(
            "^replication\\.table\\.(\\d+)\\.(source|sink)$");


    private EnvironmentVariableEvaluator envEvaluator = new EnvironmentVariableEvaluator();

    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public List<ReplicationTable> getReplicationTables() {
        Map<Integer, String[]> tableValues = new TreeMap<>();

        for (String key : properties.stringPropertyNames()) {
            if (!key.startsWith(REPLICATION_TABLE_PREFIX)) {
                continue;
            }

            Matcher matcher = REPLICATION_TABLE_PROPERTY.matcher(key);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid replication table property: " + key);
            }

            final int index;
            try {
                index = Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Replication table index is out of range: " + key, e);
            }

            if (index <= 0) {
                throw new IllegalArgumentException("Replication table index must be greater than zero: " + key);
            }

            String value = properties.getProperty(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Replication table property must not be blank: " + key);
            }

            String[] pair = tableValues.computeIfAbsent(index, ignored -> new String[2]);
            pair["source".equals(matcher.group(2)) ? 0 : 1] = value;
        }

        if (tableValues.isEmpty()) {
            return List.of();
        }

        List<ReplicationTable> replicationTables = new ArrayList<>(tableValues.size());
        long expectedIndex = 1;
        for (Map.Entry<Integer, String[]> entry : tableValues.entrySet()) {
            if (entry.getKey() != expectedIndex) {
                throw new IllegalArgumentException(
                        "Replication table indexes must be contiguous starting at 1; missing index "
                                + expectedIndex);
            }

            String[] pair = entry.getValue();
            if (pair[0] == null || pair[0].isBlank()) {
                throw new IllegalArgumentException(
                        "Missing source table for replication table index " + entry.getKey());
            }
            if (pair[1] == null || pair[1].isBlank()) {
                throw new IllegalArgumentException(
                        "Missing sink table for replication table index " + entry.getKey());
            }

            replicationTables.add(new ReplicationTable(pair[0], pair[1]));
            expectedIndex++;
        }

        return List.copyOf(replicationTables);
    }

    public OptionsFile(String optionsFilePath) throws IOException {
        this.properties = new Properties();
        loadProperties(optionsFilePath);
    }

    private void loadProperties(String optionsFilePath) throws IOException {

        // open reader to read the properties file
        try (FileReader in = new FileReader(optionsFilePath)) {
            // load the properties from that reader
            Properties loadedProperties = new ReplicationProperties();
            loadedProperties.load(in);
            ((ReplicationProperties) loadedProperties).allowPropertyUpdates();
            this.properties = loadedProperties;
            resolvePropertiesEnvVar();
        } catch (IOException e) {
            // handle the exception
            LOG.error(e);
            throw e;
        }
    }

    private static final class ReplicationProperties extends Properties {

        private boolean rejectDuplicates = true;

        private void allowPropertyUpdates() {
            rejectDuplicates = false;
        }

        @Override
        public synchronized Object put(Object key, Object value) {
            if (key instanceof String propertyName
                    && rejectDuplicates
                    && propertyName.startsWith(REPLICATION_TABLE_PREFIX)
                    && containsKey(key)) {
                throw new IllegalArgumentException("Duplicate replication table property: " + propertyName);
            }
            return super.put(key, value);
        }
    }

    public Properties getSourceConnectionParams() {

        Set<Object> propertyKeys = this.properties.keySet();
        Properties sourceConnectProps = new Properties();
        String connectionProperty;
        String value;

        for (Object propertyKey : propertyKeys) {
            String key = (String) propertyKey;

            if (key.startsWith(SOURCE_CONNECTION_PREFIX)) {
                connectionProperty = key.substring(SOURCE_CONNECTION_PREFIX.length());
                value = this.properties.getProperty(key);
                sourceConnectProps.setProperty(connectionProperty, value);
            }
        }

        return sourceConnectProps;

    }

    public Properties getSinkConnectionParams() {
        Set<Object> propertyKeys = this.properties.keySet();
        Properties sinkConnectProps = new Properties();
        String connectionProperty;
        String value;

        for (Object propertyKey : propertyKeys) {
            String key = (String) propertyKey;

            if (key.startsWith(SINK_CONNECTION_PREFIX)) {
                connectionProperty = key.substring(SINK_CONNECTION_PREFIX.length());
                value = this.properties.getProperty(key);
                sinkConnectProps.setProperty(connectionProperty, value);
            }
        }

        return sinkConnectProps;
    }

    private void resolvePropertiesEnvVar() {
        Enumeration<?> propertyNames = this.properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            String value = this.properties.getProperty(name);

            if (value != null && !value.isEmpty())
                this.properties.setProperty(name, envEvaluator.resolveEnvVars(value));

        }
    }


    public void printProperties() {
        // print out what you just read
        Properties redactedProperties = CredentialRedactor.redactProperties(properties);
        Enumeration<?> propertyNames = redactedProperties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            System.out.println(name + "=" + redactedProperties.getProperty(name));
        }
    }
}
