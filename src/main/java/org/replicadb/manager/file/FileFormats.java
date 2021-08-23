package org.replicadb.manager.file;

public enum FileFormats {
    JSON("json"), CSV("csv"), AVRO("avro"), PARQUET("parquet"), ORC("orc");

    private final String type;

    FileFormats(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
