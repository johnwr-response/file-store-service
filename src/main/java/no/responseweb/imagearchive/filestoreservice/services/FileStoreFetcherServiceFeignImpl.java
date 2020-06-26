package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class FileStoreFetcherServiceFeignImpl implements FileStoreFetcherService {

    public static final String FETCH_PATH = "api/v1/fetchFile/{fileItemId}";

    private final FileStoreFetcherServiceFeignClient fileStoreFetcherServiceFeignClient;

    @Override
    public byte[] fetchFile(UUID fileItemId) {
        ResponseEntity<byte[]> responseEntity = fileStoreFetcherServiceFeignClient.fetchFile(fileItemId);
        return responseEntity.getBody();
    }
}
