package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.services.*;
import no.responseweb.imagearchive.filestoreservice.config.JmsConfig;
import no.responseweb.imagearchive.model.*;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileStoreListener {

    private final FileStoreService fileStoreService;
    private final FilePathService filePathService;
    private final FileItemService fileItemService;
    private final FileEntityService fileEntityService;
    private final StatusWalkerService statusWalkerService;

    private final JmsTemplate jmsTemplate;

    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) {
        if (fileStoreRequest != null && fileStoreRequest.getFileStore() != null && fileStoreRequest.getFilePath() != null && fileStoreRequest.getFileItem() != null) {
            FileStoreDto fileStoreDto = fileStoreRequest.getFileStore();
            FilePathDto filePathDto = fileStoreRequest.getFilePath();
            FileItemDto fileItemDto = fileStoreRequest.getFileItem();

            // Load data with existing IDs. (When rebuilding database)
            fileStoreDto = saveIfNotPresent(fileStoreDto);
            filePathDto = saveIfNotPresent(filePathDto);
            fileItemDto = saveIfNotPresent(fileItemDto);

            fileStoreDto = createStoreIfNew(fileStoreDto);
            filePathDto = createPathIfNew(fileStoreDto, filePathDto);
            fileItemDto.setFileStorePathId((filePathDto==null) ? null : filePathDto.getId());
            fileItemDto = saveAndFetch(fileItemDto);

            switch (fileStoreRequest.getFileStoreRequestType()) {
                case DELETE:
                    fileEntityService.deleteById(fileItemDto.getFileEntityId());
                    fileItemService.delete(fileItemDto);
                    log.info("Deleted file: {}", fileItemDto);
                    break;
                case INSERT:
                    if (fileItemDto.getId() == null) {
                        fileItemDto = fileItemService.save(fileItemDto);
                        log.info("Inserted file: {}", fileItemDto);
                    }
                    break;
                case UPDATE:
                    fileItemDto = fileItemService.save(fileItemDto);
                    log.info("Updated file: {}", fileItemDto);
                    break;
                default:
                    log.info("Is this reachable?: {}", fileItemDto);
                    break;
            }
            // fetch file-item, extract metadata, save to database
            if (fileItemDto != null && fileItemDto.getId()!=null) {
                jmsTemplate.convertAndSend(
                        JmsConfig.FILE_PROCESSING_JOB_QUEUE, FileProcessingJobDto.builder()
                                .fileStoreRequestType(fileStoreRequest.getFileStoreRequestType())
                                .config(new FileProcessingJobForwarderConfig(
                                        FileProcessingJobForwarderConfig.Config.IMAGES))
                                .fileItemDto(fileItemDto)
                                .build()
                );

                StatusWalkerDto statusWalkerDto = statusWalkerService.findFirstByWalkerInstanceTokenAndFileStoreId(
                        fileStoreRequest.getWalkerJobDto().getWalkerInstanceToken(),
                        fileStoreDto.getId()
                );
                statusWalkerDto.setLastActiveDate(LocalDateTime.now());
                statusWalkerService.save(statusWalkerDto);
            }
        }

    }
    private FileStoreDto saveIfNotPresent(FileStoreDto fileStoreDto) {
        return (fileStoreService.findFirstById(fileStoreDto)==null) ? fileStoreService.save(fileStoreDto) : fileStoreDto;
    }
    private FilePathDto saveIfNotPresent(FilePathDto filePathDto) {
        return (filePathService.findFirstById(filePathDto)==null) ? filePathService.save(filePathDto) : filePathDto;
    }
    private FileItemDto saveIfNotPresent(FileItemDto fileItemDto) {
        return (fileItemService.findFirstById(fileItemDto)==null) ? fileItemService.save(fileItemDto) : fileItemDto;
    }
    private FileStoreDto createStoreIfNew(FileStoreDto fileStoreDto) {
        if (fileStoreDto.getId() == null) {
            fileStoreDto.setLatestRefresh(LocalDateTime.now());
            fileStoreDto = fileStoreService.save(fileStoreDto);
            log.info("Created new store: {}", fileStoreDto);
        }
        return fileStoreDto;
    }
    private FilePathDto createPathIfNew(FileStoreDto fileStoreDto, FilePathDto filePathDto) {
        FilePathDto checkForUpdates = filePathService.findByFileStoreIdAndRelativePath(fileStoreDto, filePathDto);
        if (checkForUpdates!=null) {
            filePathDto = checkForUpdates;
        }
        filePathDto = filePathService.save(filePathDto);
        log.info("Created new sub-path: {}", filePathDto);
        return filePathDto;
    }
    private FileItemDto saveAndFetch(FileItemDto fileItemDto) {
        return fileItemService.save(fileItemDto);
    }
}
