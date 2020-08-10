package it.carloni.luca.aurora.utils;

import java.time.format.DateTimeFormatter;

public enum DateFormat {

    DT_BUSINESS_DATE("yyyy-MM-dd", DateTimeFormatter.ISO_LOCAL_DATE);

    private final String format;
    private final DateTimeFormatter formatter;

    public DateTimeFormatter getFormatter() { return formatter; }
    public String getFormat() { return format; }

    DateFormat(String format, DateTimeFormatter dateTimeFormatter) {

        this.format = format;
        this.formatter = dateTimeFormatter;
    }
}
