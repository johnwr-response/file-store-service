package no.responseweb.imagearchive.filestoreservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "response.walker.status", ignoreUnknownFields = false)
public class ResponseWalkerStatusProperties {
    private int cutoffHours = 24;
    private int globalStoreRefreshMinutes = 3600*24;
}
