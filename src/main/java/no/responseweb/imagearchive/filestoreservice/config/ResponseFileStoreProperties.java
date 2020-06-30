package no.responseweb.imagearchive.filestoreservice.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "response.filestore", ignoreUnknownFields = false)
public class ResponseFileStoreProperties {
    private int thumbnailSize = 200;
    private boolean thumbnailGenerate = false;
}
