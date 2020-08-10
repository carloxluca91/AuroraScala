package it.carloni.luca.aurora.option;

public enum ScoptOption {

    APPLICATION_BRANCH('b', "branch", "Application branch to run"),
    PROPERTIES_OPTION('p', "properties", "Name of .properties file"),
    SOURCE_OPTION('s', "source", "Source name (BANCLL) for which ingestion must be triggered"),
    BUSINESS_DATE_OPTION('d', "date", "Working business date"),
    MAPPING_SPECIFICATION_FLAG('m', "mapping_specification", "Flag for overwriting mapping specification table"),
    LOOKUP_SPECIFICATION_FLAG('l', "look_up", "Flag for overwriting look up table"),
    COMPLETE_OVERWRITE_FLAG('o', "overwrite", "Flag for specifing behavior when reloading tables. " +
            "True: drop and reload. False: truncate and reload"),
    VERSION_NUMBER_OPTION('v', "version", "Specification version number to be referred to");

    private final char shortOption;
    private final String longOption;
    private final String description;

    public char getShortOption() { return shortOption; }
    public String getLongOption() { return longOption; }
    public String getDescription() { return description; }

    ScoptOption(char shortOption, String longOption, String description) {

        this.shortOption = shortOption;
        this.longOption = longOption;
        this.description = description;
    }
}
