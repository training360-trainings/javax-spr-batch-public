package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;

@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
@Slf4j
public class NotifyFlowConfiguration {

    private final JobRepository jobRepository;

    @Bean
    public Flow notifyFlow(AsyncTaskExecutor batchTaskExecutor,
                           Flow emailNotifyFlow,
                           Flow chatNotifyFlow
                    ) {
        return new FlowBuilder<Flow>("notifyFlow")
                .split(batchTaskExecutor)
                .add(emailNotifyFlow, chatNotifyFlow)
                .build();
    }

    @Bean
    public Flow emailNotifyFlow(Step emailNotifyStep) {
        return new FlowBuilder<Flow>("emailNotifyFlow")
                .start(emailNotifyStep)
                .build();
    }

    @Bean
    public Step emailNotifyStep(Tasklet emailNotifyTasklet) {
        return new StepBuilder("emailNotifyStep", jobRepository)
                .tasklet(emailNotifyTasklet).build();
    }

    @Bean
    public Tasklet emailNotifyTasklet() {
        return ((contribution, chunkContext) -> {
            log.info("Running email notify tasklet");
            return RepeatStatus.FINISHED;
        }
                );
    }

    @Bean
    public Flow chatNotifyFlow(Step chatNotifyStep) {
        return new FlowBuilder<Flow>("chatNotifyFlow")
                .start(chatNotifyStep)
                .build();
    }

    @Bean
    public Step chatNotifyStep(Tasklet chatNotifyTasklet) {
        return new StepBuilder("chatNotifyStep", jobRepository)
                .tasklet(chatNotifyTasklet).build();
    }

    @Bean
    public Tasklet chatNotifyTasklet() {
        return ((contribution, chunkContext) -> {
            log.info("Running chat notify tasklet");
            return RepeatStatus.FINISHED;
        }
        );
    }
}
