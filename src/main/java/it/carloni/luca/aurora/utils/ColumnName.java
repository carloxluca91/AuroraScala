package it.carloni.luca.aurora.utils;

public enum ColumnName {

    // SPECIFICATION
    FLUSSO("flusso"),
    VERSIONE("versione"),
    DT_INSERIMENTO("dt_inserimento"),
    DT_RIFERIMENTO("dt_riferimento"),
    ERROR_DESCRIPTION("descrizione_errore"),
    ROW_COUNT("row_count"),
    ROW_ID("row_id"),
    TS_INSERIMENTO("ts_inserimento"),

    // LOOKUP
    LOOKUP_TIPO("tipo_lookup"),
    LOOKUP_ID("lookup_id"),
    LOOKUP_VALORE_ORIGINALE("valore_originale"),
    LOOKUP_VALORE_SOSTITUZIONE("valore_sostituzione");

    private final String name;

    public String getName() { return name; }

    ColumnName(String name) { this.name = name; }
}
