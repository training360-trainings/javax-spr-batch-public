package ordersbatch;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.step.StepExecution;

public class SkippedDecider implements JobExecutionDecider {

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, @Nullable StepExecution stepExecution) {
        int counter = stepExecution
                .getJobExecution()
                .getExecutionContext()
                .getInt("skipped.lines");
        if (counter == 0) {
            return FlowExecutionStatus.COMPLETED;
        }
        else {
            return new FlowExecutionStatus("PARTIALLY_COMPLETED");
        }
    }
}
