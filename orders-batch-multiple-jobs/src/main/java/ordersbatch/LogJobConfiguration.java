package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
@Slf4j
public class LogJobConfiguration {

    private final JobRepository  jobRepository;

    @Bean
    public Job logJob(Step logStep) {
        return new JobBuilder("logJob", jobRepository)
                .start(logStep)
                .build();
    }

    @Bean
    public Step logStep(Tasklet logTasklet) {
        return new StepBuilder("logStep", jobRepository)
                .tasklet(logTasklet)
                .build();
    }

    @Bean
    public Tasklet logTasklet() {
        return ((contribution, chunkContext) -> {
            log.info("Log tasklet started");
            return null;
        });
    }
}
