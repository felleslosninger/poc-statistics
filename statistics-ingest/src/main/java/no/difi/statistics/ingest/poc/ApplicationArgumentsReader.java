package no.difi.statistics.ingest.poc;

import org.springframework.boot.ApplicationArguments;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class ApplicationArgumentsReader {

    private ApplicationArguments arguments;

    public ApplicationArgumentsReader(ApplicationArguments arguments) {
        this.arguments = arguments;
    }

    public ZonedDateTime from() {
        return date("from");
    }

    public ZonedDateTime to() {
        return date("to");
    }

    private ZonedDateTime date(String argumentName) {
        if (!arguments.containsOption(argumentName)) throw new RuntimeException("Parameter \"" + argumentName + "\" missing");
        return arguments.getOptionValues(argumentName).stream().findFirst()
                .map(s -> DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(s, ZonedDateTime::from))
                .orElseThrow(() -> new RuntimeException("Parameter \"" + argumentName + "\" missing"));
    }

}
