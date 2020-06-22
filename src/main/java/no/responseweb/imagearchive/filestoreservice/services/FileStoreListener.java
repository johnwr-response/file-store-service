package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.domain.FilePath;
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
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileStoreListener {

    private final FileStoreRepository fileStoreRepository;
    private final FilePathRepository filePathRepository;
    private final FileItemRepository fileItemRepository;
    private final FileStoreMapper fileStoreMapper;
    private final FilePathMapper filePathMapper;
    private final FileItemMapper fileItemMapper;

    @JmsListener(destination = JmsConfig.FILE_STORE_REQUEST_QUEUE)
    public void listen(FileStoreRequestDto fileStoreRequest) {
        FileStoreDto fileStoreDto = fileStoreRequest.getFileStore();
        FilePathDto filePathDto = fileStoreRequest.getFilePath();
        FileItemDto fileItemDto = fileStoreRequest.getFileItem();
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
                log.info("Deleted this {} {} {}. FileStoreRequest: {}", fileStoreDto.getLocalBaseUri(), filePathDto.getRelativePath(), fileItemDto.getFilename(), fileStoreRequest);
                break;
            case INSERT:
                FileItemDto insertedFileItemDto = fileItemMapper.fileItemToFileItemDto(fileItemRepository.save(fileItemMapper.fileItemDtoToFileItem(fileItemDto)));
                log.info("Inserted this {} {} {}. FileStoreRequest: {}", fileStoreDto.getLocalBaseUri(), filePathDto.getRelativePath(), fileItemDto.getFilename(), fileStoreRequest);
                break;
            case UPDATE:
                log.info("Unmapped: {}", fileItemDto);
                log.info("Mapped Created: {}, Updated: {}", fileItemMapper.fileItemDtoToFileItem(fileItemDto).getCreatedDate(), fileItemMapper.fileItemDtoToFileItem(fileItemDto).getLastModifiedDate());
                FileItemDto updatedFileItemDto = fileItemMapper.fileItemToFileItemDto(fileItemRepository.save(fileItemMapper.fileItemDtoToFileItem(fileItemDto)));
                log.info("Updated this {} {} {}. FileStoreRequest: {}", fileStoreDto.getLocalBaseUri(), filePathDto.getRelativePath(), fileItemDto.getFilename(), fileStoreRequest);
                break;
            default:
                break;
        }
    }

}
