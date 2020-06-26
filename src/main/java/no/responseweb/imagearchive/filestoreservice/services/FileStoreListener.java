package no.responseweb.imagearchive.filestoreservice.services;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.domain.*;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileItemMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FilePathMapper;
import no.responseweb.imagearchive.filestoredbservice.mappers.FileStoreMapper;
import no.responseweb.imagearchive.filestoredbservice.repositories.*;
import no.responseweb.imagearchive.filestoreservice.config.JmsConfig;
import no.responseweb.imagearchive.filestoreservice.config.ResponseWalkerStatusProperties;
import no.responseweb.imagearchive.model.*;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;

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
    private final ImageMetadataCollectionRepository imageMetadataCollectionRepository;
    private final ImageMetadataDirectoryRepository imageMetadataDirectoryRepository;
    private final ImageMetadataTagRepository imageMetadataTagRepository;
    private final ImageMetadataValueRepository imageMetadataValueRepository;
    private final StatusWalkerRepository statusWalkerRepository;

    private final ResponseWalkerStatusProperties responseWalkerStatusProperties;

    private final FileStoreFetcherService fileStoreFetcherService;

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
        deleteOldStatusWalkers(walkerStatusReportDto);

    }
    private void deleteOldStatusWalkers(WalkerStatusReportDto walkerStatusReportDto) {
        statusWalkerRepository.deleteInBatch(
                statusWalkerRepository.findAllByFileStoreIdAndLastActiveDateIsBefore(
                        walkerStatusReportDto.getFileStoreId(),
                        LocalDateTime.now().minusHours(responseWalkerStatusProperties.getCutoffHours())
                )
        );
    }

    // TODO: Reduce Cognitive Complexity
    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) throws IOException, ImageProcessingException {
        FileStoreDto fileStoreDto = fileStoreRequest.getFileStore();
        FilePathDto filePathDto = fileStoreRequest.getFilePath();
        FileItemDto fileItemDto = fileStoreRequest.getFileItem();
        if (fileStoreDto.getId() == null) {
            fileStoreDto.setLatestRefresh(LocalDateTime.now());
            fileStoreDto = fileStoreMapper.fileStoreToFileStoreDto(
                    fileStoreRepository.save(
                            fileStoreMapper.fileStoreDtoToFileStore(fileStoreDto)
                    )
            );
        }
        if (filePathDto.getId() == null) {
            FilePath checkForUpdates = filePathRepository.findByFileStoreIdAndRelativePath(fileStoreDto.getId(), (filePathDto.getRelativePath()!=null?filePathDto.getRelativePath():""));
            if (checkForUpdates!=null) {
                filePathDto = filePathMapper.filePathToFilePathDto(checkForUpdates);
            }
            filePathDto = filePathMapper.filePathToFilePathDto(filePathRepository.saveAndFlush(filePathMapper.filePathDtoToFilePath(filePathDto)));
            fileItemDto.setFileStorePathId(filePathDto.getId());
            log.info("Created new sub-path: {}", fileItemDto);
        }

        switch (fileStoreRequest.getFileStoreRequestType()) {
            case DELETE:
                fileItemRepository.delete(fileItemMapper.fileItemDtoToFileItem(fileItemDto));
                break;
            case INSERT:
            case UPDATE:
                fileItemDto = fileItemMapper.fileItemToFileItemDto(fileItemRepository.save(fileItemMapper.fileItemDtoToFileItem(fileItemDto)));
                break;
            default:
                break;
        }
        // fetch file-item, extract metadata, save to database
        if (fileItemDto.getId()!=null) {
            byte[] downloadedBytes = fileStoreFetcherService.fetchFile(fileItemDto.getId());
            log.info("File: {}, Size: {}", fileItemDto.getFilename(), downloadedBytes.length);
            BufferedImage image = ImageIO.read(new ByteArrayInputStream(downloadedBytes));
            if (image!=null) {
                Metadata metadata = ImageMetadataReader.readMetadata(new ByteArrayInputStream(downloadedBytes));
                for (Directory directory : metadata.getDirectories()) {
                    ImageMetadataDirectory currDir = imageMetadataDirectoryRepository.findFirstByName(directory.getName());
                    if (currDir == null) {
                        currDir = imageMetadataDirectoryRepository.save(ImageMetadataDirectory.builder()
                                .collectionId(imageMetadataCollectionRepository.findFirstByName("unset").getId())
                                .name(directory.getName())
                                .build());
                    }
                    for (Tag tag : directory.getTags()) {
                        ImageMetadataTag currTag = imageMetadataTagRepository.findFirstByDirectoryIdAndKeyName(currDir.getId(), tag.getTagName());
                        if (currTag == null) {
                            currTag = imageMetadataTagRepository.save(ImageMetadataTag.builder()
                                    .directoryId(currDir.getId())
                                    .keyName(tag.getTagName())
                                    .tagDec(tag.getTagType())
                                    .build());
                        }
                        ImageMetadataValue currValue = imageMetadataValueRepository.findFirstByTagIdAndFileItemId(currTag.getId(), fileItemDto.getId());
                        if (currValue == null) {
                            currValue = imageMetadataValueRepository.save(ImageMetadataValue.builder()
                                    .tagId(currTag.getId())
                                    .fileItemId(fileItemDto.getId())
                                    .value(tag.getDescription())
                                    .build());
                        } else if (!currValue.getValue().equals(tag.getDescription())) {
                            currValue.setValue(tag.getDescription());
                            imageMetadataValueRepository.save(currValue);
                        }
                        log.info("File.name: {}, Directory.name: {}, Tag.type: {}, Tag.name: {}, Tag.description: {}", fileItemDto.getFilename(), currDir.getName(), currTag.getTagDec(), currTag.getKeyName(), currValue.getValue());
                    }
                }
            } else {
                log.info("Not an Image: {}", fileItemDto.getFilename());
            }
            StatusWalker statusWalker = statusWalkerRepository.getOne(fileStoreRequest.getWalkerJobDto().getWalkerInstanceToken());
            statusWalker.setLastActiveDate(LocalDateTime.now());
            statusWalkerRepository.save(statusWalker);
        }
    }

}
