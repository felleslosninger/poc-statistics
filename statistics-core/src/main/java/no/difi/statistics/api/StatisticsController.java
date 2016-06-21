package no.difi.statistics.api;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatisticsController {

    @RequestMapping("/")
    public String index() {
        return "Statistics API";
    }

}
