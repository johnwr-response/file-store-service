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
    private int cutoffMinutes = 30;
    private int disconnectSeconds = 30;
    private int globalStoreRefreshMinutes = 3600*24;
}
