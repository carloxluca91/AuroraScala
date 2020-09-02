package it.carloni.luca.aurora.utils;

public enum TableId {

    MAPPING_SPECIFICATION("mapping_specification"),
    LOOK_UP("lookup");

    private final String id;

    public String getId() { return id; }

    TableId(String id) { this.id = id; }
}
