package no.responseweb.imagearchive.filestoreservice.services;

import java.util.UUID;

public interface FileStoreFetcherService {
    byte[] fetchFile(UUID fileItemId);
}
