package it.carloni.luca.aurora.option;


public enum Branch {

    INITIAL_LOAD("INITIAL_LOAD"),
    RE_LOAD("RE_LOAD"),
    SOURCE_LOAD("SOURCE_LOAD");

    private final String name;

    public String getName() { return name; }

    Branch(String name) { this.name = name; }
}
