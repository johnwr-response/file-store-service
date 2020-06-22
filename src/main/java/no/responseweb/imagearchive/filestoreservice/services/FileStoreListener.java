package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.domain.FilePath;
import no.responseweb.imagearchive.filestoredbservice.domain.FileStore;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileItemMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FilePathMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileStoreMapper;
import no.responseweb.imagearchive.filestoredbservice.repositories.FileItemRepository;
import no.responseweb.imagearchive.filestoredbservice.repositories.FilePathRepository;
import no.responseweb.imagearchive.filestoredbservice.repositories.FileStoreRepository;
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
public class FileStoreListener {

    private final JmsTemplate jmsTemplate;
    // TODO: Move all database config and executions into separate db module. Then enable these
    private final FileStoreRepository fileStoreRepository;
    private final FilePathRepository filePathRepository;
    private final FileItemRepository fileItemRepository;
    private final FileStoreMapper fileStoreMapper;
    private final FilePathMapper filePathMapper;
    private final FileItemMapper fileItemMapper;

    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) {
        FileStoreDto fileStore = fileStoreRequest.getFileStore();
        FilePathDto filePath = fileStoreRequest.getFilePath();
        FileItemDto fileItem = fileStoreRequest.getFileItem();
        if (filePath.getId() == null) {
            FileStore byNickname = fileStoreRepository.findByNickname(fileStore.getNickname());
            log.info("Store: {}", fileStoreMapper.fileStoreToFileStoreDto(byNickname));
            FilePath createdFilePath = filePathRepository.saveAndFlush(filePathMapper.filePathDtoToFilePath(filePath));
            fileItem.setFileStorePathId(createdFilePath.getId());
            filePath = filePathMapper.filePathToFilePathDto(createdFilePath);
            log.info("Created new sub-path: {}", filePathMapper.filePathToFilePathDto(createdFilePath));
        }

        switch (fileStoreRequest.getFileStoreRequestType()) {
            case DELETE:
                fileItemRepository.delete(fileItemMapper.fileItemDtoToFileItem(fileItem));
                log.info("Deleted this {} {} {}. FileStoreRequest: {}", fileStore.getLocalBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            case INSERT:
                fileItemRepository.saveAndFlush(fileItemMapper.fileItemDtoToFileItem(fileItem));
                log.info("Inserted this {} {} {}. FileStoreRequest: {}", fileStore.getLocalBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            case UPDATE:
                log.info("Unmapped: {}", fileItem);
                log.info("Mapped: {}", fileItemMapper.fileItemDtoToFileItem(fileItem).getLastModifiedDate());
                fileItemRepository.saveAndFlush(fileItemMapper.fileItemDtoToFileItem(fileItem));
                log.info("Updated this {} {} {}. FileStoreRequest: {}", fileStore.getLocalBaseUri(), filePath.getRelativePath(), fileItem.getFilename(), fileStoreRequest);
                break;
            default:
                break;
        }
    }

}
