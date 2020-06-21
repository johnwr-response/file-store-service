package no.responseweb.imagearchive.filestoreservice;

import no.responseweb.imagearchive.filestoredbservice.config.DBModuleConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(DBModuleConfig.class)
public class FileStoreServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(FileStoreServiceApplication.class, args);
	}

}
