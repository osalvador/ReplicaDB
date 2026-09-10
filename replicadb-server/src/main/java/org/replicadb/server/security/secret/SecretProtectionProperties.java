package org.replicadb.server.security.secret;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "replicadb.security")
public class SecretProtectionProperties {

    public static final String KEYRING_FILE_PROPERTY = "replicadb.security.keyring.file";
    public static final String MASTER_KEY_FILE_PROPERTY = "replicadb.security.master-key-file";
    public static final String DEFAULT_KEYRING_FILE = "/run/secrets/replicadb-master-key";

    private Keyring keyring = new Keyring();

    public Keyring getKeyring() {
        return keyring;
    }

    public void setKeyring(Keyring keyring) {
        this.keyring = keyring;
    }

    public static class Keyring {

        private String file = DEFAULT_KEYRING_FILE;
        private Slot current = new Slot();
        private Slot secondary = new Slot();

        public String getFile() {
            return file;
        }

        public void setFile(String file) {
            this.file = file;
        }

        public Slot getCurrent() {
            return current;
        }

        public void setCurrent(Slot current) {
            this.current = current;
        }

        public Slot getSecondary() {
            return secondary;
        }

        public void setSecondary(Slot secondary) {
            this.secondary = secondary;
        }
    }

    public static class Slot {

        private String version = "";
        private String key = "";

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }
    }
}
