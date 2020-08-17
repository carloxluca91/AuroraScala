package it.carloni.luca.aurora.utils;

public enum ColumnName {

    DT_BUSINESS_DATE("dt_business_date"),
    ROW_ID("row_id");

    private final String name;

    public String getName() { return name; }

    ColumnName(String name) { this.name = name; }
}
