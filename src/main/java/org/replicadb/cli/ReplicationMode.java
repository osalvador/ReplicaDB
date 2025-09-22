package org.replicadb.cli;

public enum ReplicationMode {
    COMPLETE("complete"),
    INCREMENTAL("incremental"),
    COMPLETE_ATOMIC("complete-atomic");

    private final String modeText;


    ReplicationMode(String modeText) {
        this.modeText = modeText;
    }

    public String getModeText() {
        return modeText;
    }

}
