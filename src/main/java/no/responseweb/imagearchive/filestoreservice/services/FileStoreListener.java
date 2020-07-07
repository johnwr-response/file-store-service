package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.domain.FileEntity;
import no.responseweb.imagearchive.filestoredbservice.domain.FilePath;
import no.responseweb.imagearchive.filestoredbservice.domain.StatusWalker;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileEntityMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileItemMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FilePathMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileStoreMapper;
import no.responseweb.imagearchive.filestoredbservice.repositories.*;
import no.responseweb.imagearchive.filestoreservice.config.JmsConfig;
import no.responseweb.imagearchive.filestoreservice.config.ResponseWalkerStatusProperties;
import no.responseweb.imagearchive.model.*;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileStoreListener {

    private final FileStoreRepository fileStoreRepository;
    private final FileStoreMapper fileStoreMapper;
    private final FilePathRepository filePathRepository;
    private final FilePathMapper filePathMapper;
    private final FileItemRepository fileItemRepository;
    private final FileItemMapper fileItemMapper;
    private final FileEntityRepository fileEntityRepository;
    private final FileEntityMapper fileEntityMapper;
    private final StatusWalkerRepository statusWalkerRepository;

    private final ResponseWalkerStatusProperties responseWalkerStatusProperties;

    private final JmsTemplate jmsTemplate;

    @JmsListener(destination = JmsConfig.FILE_STORE_WALKER_STATUS_QUEUE)
    public void listen(WalkerStatusReportDto walkerStatusReportDto) {
        StatusWalker statusWalker = statusWalkerRepository.findFirstByWalkerInstanceTokenAndFileStoreId(
                walkerStatusReportDto.getWalkerInstanceToken(),
                walkerStatusReportDto.getFileStoreId()
        );
        if (statusWalker==null) {
            statusWalker = StatusWalker.builder()
                    .walkerInstanceToken(walkerStatusReportDto.getWalkerInstanceToken())
                    .fileStoreId(walkerStatusReportDto.getFileStoreId())
                    .ready(walkerStatusReportDto.isReady())
                    .lastActiveDate(LocalDateTime.now())
                    .build();
        } else {
            statusWalker.setReady(walkerStatusReportDto.isReady());
            statusWalker.setLastActiveDate(LocalDateTime.now());
        }
        statusWalkerRepository.save(statusWalker);
        disconnectOldStatusWalkers(walkerStatusReportDto);
        deleteOldStatusWalkers(walkerStatusReportDto);

    }
    private void disconnectOldStatusWalkers(WalkerStatusReportDto walkerStatusReportDto) {
        List<StatusWalker> disconnectedWalkers = statusWalkerRepository.findAllByFileStoreIdAndLastActiveDateIsBefore(
                walkerStatusReportDto.getFileStoreId(),
                LocalDateTime.now().minusSeconds(responseWalkerStatusProperties.getDisconnectSeconds())
        );
        disconnectedWalkers.forEach(statusWalker -> {
            statusWalker.setReady(false);
            statusWalkerRepository.save(statusWalker);
        });

    }
    private void deleteOldStatusWalkers(WalkerStatusReportDto walkerStatusReportDto) {
        statusWalkerRepository.deleteInBatch(
                statusWalkerRepository.findAllByFileStoreIdAndLastActiveDateIsBefore(
                        walkerStatusReportDto.getFileStoreId(),
                        LocalDateTime.now().minusMinutes(responseWalkerStatusProperties.getCutoffMinutes())
                )
        );
    }

    // TODO: Reduce Cognitive Complexity
    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) {
        if (fileStoreRequest != null && fileStoreRequest.getFileStore() != null && fileStoreRequest.getFilePath() != null && fileStoreRequest.getFileItem() != null) {
            FileStoreDto fileStoreDto = fileStoreRequest.getFileStore();
            FilePathDto filePathDto = fileStoreRequest.getFilePath();
            FileItemDto fileItemDto = fileStoreRequest.getFileItem();

            if (fileItemDto.getFileEntityId()==null) {
                FileEntity fileEntity = fileEntityRepository.saveAndFlush(FileEntity.builder()
                        .id(fileItemDto.getId())
                        .build());
                fileItemDto.setFileEntityId(fileEntity.getId());
            }

            // Load data with existing IDs. (When rebuilding database)
            if (fileStoreDto.getId()!=null && fileStoreRepository.findFirstById(fileStoreDto.getId())==null) {
                fileStoreRepository.saveAndFlush(fileStoreMapper.fileStoreDtoToFileStore(fileStoreDto));
            }
            if (filePathDto.getId()!=null && filePathRepository.findFirstById(filePathDto.getId())==null) {
                filePathRepository.saveAndFlush(filePathMapper.filePathDtoToFilePath(filePathDto));
            }
            if (fileItemDto.getId()!=null && fileItemRepository.findFirstById(fileItemDto.getId())==null) {
                fileItemRepository.saveAndFlush(fileItemMapper.fileItemDtoToFileItem(fileItemDto));
            }

            if (fileStoreDto.getId() == null) {
                fileStoreDto.setLatestRefresh(LocalDateTime.now());
                fileStoreDto = fileStoreMapper.fileStoreToFileStoreDto(
                        fileStoreRepository.saveAndFlush(
                                fileStoreMapper.fileStoreDtoToFileStore(fileStoreDto)
                        )
                );
                log.info("Created new store: {}", fileStoreDto);
            }
            if (filePathDto.getId() == null) {
                FilePath checkForUpdates = filePathRepository.findByFileStoreIdAndRelativePath(fileStoreDto.getId(), (filePathDto.getRelativePath()!=null?filePathDto.getRelativePath():""));
                if (checkForUpdates!=null) {
                    filePathDto = filePathMapper.filePathToFilePathDto(checkForUpdates);
                }
                filePathDto = filePathMapper.filePathToFilePathDto(filePathRepository.saveAndFlush(filePathMapper.filePathDtoToFilePath(filePathDto)));
                fileItemDto.setFileStorePathId(filePathDto.getId());
                log.info("Created new sub-path: {}", filePathDto);
            }

            switch (fileStoreRequest.getFileStoreRequestType()) {
                case DELETE:
                    fileEntityRepository.deleteById(fileItemDto.getFileEntityId());
                    fileItemRepository.delete(fileItemMapper.fileItemDtoToFileItem(fileItemDto));
                    log.info("Deleted file: {}", fileItemDto);
                    break;
                case INSERT:
                    if (fileItemDto.getId() == null) {
                        fileItemDto = fileItemMapper.fileItemToFileItemDto(fileItemRepository.saveAndFlush(fileItemMapper.fileItemDtoToFileItem(fileItemDto)));
                        log.info("Inserted file: {}", fileItemDto);
                    }
                    break;
                case UPDATE:
                    fileItemDto = fileItemMapper.fileItemToFileItemDto(fileItemRepository.saveAndFlush(fileItemMapper.fileItemDtoToFileItem(fileItemDto)));
                    log.info("Updated file: {}", fileItemDto);
                    break;
                default:
                    break;
            }
            // fetch file-item, extract metadata, save to database
            if (fileItemDto.getId()!=null) {
                jmsTemplate.convertAndSend(
                        JmsConfig.FILE_PROCESSING_JOB_QUEUE, FileProcessingJobDto.builder()
                                .fileStoreRequestType(fileStoreRequest.getFileStoreRequestType())
                                .config(new FileProcessingJobForwarderConfig(
                                        FileProcessingJobForwarderConfig.Config.IMAGES))
                                .fileItemDto(
                                        fileItemMapper.fileItemToFileItemDto(
                                                fileItemRepository.findFirstById(fileItemDto.getId())
                                        ))
                                .build()
                );

                StatusWalker statusWalker = statusWalkerRepository.findFirstByWalkerInstanceTokenAndFileStoreId(
                        fileStoreRequest.getWalkerJobDto().getWalkerInstanceToken(),
                        fileStoreDto.getId()
                );
                statusWalker.setLastActiveDate(LocalDateTime.now());
                statusWalkerRepository.save(statusWalker);
            }
        }

    }

}
