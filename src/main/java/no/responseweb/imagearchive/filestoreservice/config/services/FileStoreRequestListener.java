package no.responseweb.imagearchive.filestoreservice.config.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoreservice.config.JmsConfig;
import no.responseweb.imagearchive.model.FileItemDto;
import no.responseweb.imagearchive.model.FilePathDto;
import no.responseweb.imagearchive.model.FileStoreDto;
import no.responseweb.imagearchive.model.FileStoreRequestDto;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileStoreRequestListener {

    private final JmsTemplate jmsTemplate;
    // TODO: Move all database config and executions into separate db module. Then enable these
//    private final FileStoreRepository fileStoreRepository;
//    private final FilePathRepository filePathRepository;
//    private final FileItemRepository fileItemRepository;


    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) {
        FileStoreDto fileStore = fileStoreRequest.getFileStore();
        FilePathDto filePath = fileStoreRequest.getFilePath();
        FileItemDto fileItem = fileStoreRequest.getFileItem();
        if (filePath.getId() == null) {
            log.info("Crete new sub-path: {}", filePath);
        }

        switch (fileStoreRequest.getFileStoreRequestType()) {
            case DELETE:
                log.info("Delete this {} {} {}. FileStoreRequest: {}", fileStore.getBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            case INSERT:
                log.info("Insert this {} {} {}. FileStoreRequest: {}", fileStore.getBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            case UPDATE:
                log.info("Update this {} {} {}. FileStoreRequest: {}", fileStore.getBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            default:
                break;
        }
    }

}
