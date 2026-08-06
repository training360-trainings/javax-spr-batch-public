package ordersbatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.batch.autoconfigure.JobExecutionEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class JobExecutionEventListener {

    @EventListener
    public void handleJobExecutionEvent(JobExecutionEvent event) {
        log.info("Job execution event: {}", event);
    }
}
