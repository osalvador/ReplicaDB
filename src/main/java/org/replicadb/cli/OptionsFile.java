package org.replicadb.cli;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

public class OptionsFile {

    // TODO: getSourceConnectionParams and getSinkConnectionParams

    private static final Logger LOG = LogManager.getLogger(OptionsFile.class.getName());

    private EnvironmentVariableEvaluator envEvaluator = new EnvironmentVariableEvaluator();

    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public OptionsFile(String optionsFilePath) throws IOException {
        this.properties = new Properties();
        loadProperties(optionsFilePath);
    }

    private void loadProperties(String optionsFilePath) throws IOException {

        // open reader to read the properties file
        try (FileReader in = new FileReader(optionsFilePath)) {
            // load the properties from that reader
            this.properties.load(in);
            resolvePropertiesEnvVar();
        } catch (IOException e) {
            // handle the exception
            LOG.error(e);
            throw e;
        }
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
        Enumeration<?> propertyNames = properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            System.out.println(name + "=" + properties.getProperty(name));
        }
    }
}
