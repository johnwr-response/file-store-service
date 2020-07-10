package no.responseweb.imagearchive.filestoreservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.responseweb.imagearchive.filestoredbservice.services.StatusWalkerService;
import no.responseweb.imagearchive.filestoreservice.config.JmsConfig;
import no.responseweb.imagearchive.filestoreservice.config.ResponseWalkerStatusProperties;
import no.responseweb.imagearchive.model.StatusWalkerDto;
import no.responseweb.imagearchive.model.WalkerStatusReportDto;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Component
public class FileStoreWalkerStatusListener {

    private final StatusWalkerService statusWalkerService;

    private final ResponseWalkerStatusProperties responseWalkerStatusProperties;

    @JmsListener(destination = JmsConfig.FILE_STORE_WALKER_STATUS_QUEUE)
    public void listen(WalkerStatusReportDto walkerStatusReportDto) {
        StatusWalkerDto statusWalkerDto = statusWalkerService.findFirstByWalkerInstanceTokenAndFileStoreId(
                walkerStatusReportDto.getWalkerInstanceToken(),
                walkerStatusReportDto.getFileStoreId()
        );
        if (statusWalkerDto==null) {
            statusWalkerDto = StatusWalkerDto.builder()
                    .walkerInstanceToken(walkerStatusReportDto.getWalkerInstanceToken())
                    .fileStoreId(walkerStatusReportDto.getFileStoreId())
                    .ready(walkerStatusReportDto.isReady())
                    .lastActiveDate(LocalDateTime.now())
                    .build();
        } else {
            statusWalkerDto.setReady(walkerStatusReportDto.isReady());
            statusWalkerDto.setLastActiveDate(LocalDateTime.now());
        }
        statusWalkerService.save(statusWalkerDto);
        disconnectOldStatusWalkers(walkerStatusReportDto);
        deleteOldStatusWalkers(walkerStatusReportDto);

    }
    private void disconnectOldStatusWalkers(WalkerStatusReportDto walkerStatusReportDto) {
        List<StatusWalkerDto> disconnectedWalkers = statusWalkerService.findAllByFileStoreIdAndLastActiveDateIsBefore(
                walkerStatusReportDto.getFileStoreId(),
                LocalDateTime.now().minusSeconds(responseWalkerStatusProperties.getDisconnectSeconds())
        );
        disconnectedWalkers.forEach(statusWalkerDto -> {
            statusWalkerDto.setReady(false);
            statusWalkerService.save(statusWalkerDto);
        });

    }
    private void deleteOldStatusWalkers(WalkerStatusReportDto walkerStatusReportDto) {
        statusWalkerService.deleteAllByFileStoreIdAndLastActiveDateIsBeforeInBatch(
                walkerStatusReportDto.getFileStoreId(),
                LocalDateTime.now().minusMinutes(responseWalkerStatusProperties.getCutoffMinutes())
        );
    }
}
