package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
public class BatchShutdownListener {

    private final JobOperator jobOperator;

    private final JobRepository jobRepository;

    private final Job ordersJob;


    @EventListener
    @SneakyThrows
    public void onApplicationEvent(ContextClosedEvent event) {
        log.info("Shutting down application");
        Set<JobExecution> executions = jobRepository.findRunningJobExecutions(ordersJob.getName());
        for (JobExecution jobExecution : executions) {
            jobOperator.stop(jobExecution);
        }
        log.info("Application stopped");
    }
}
