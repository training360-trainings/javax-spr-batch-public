package ordersbatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@TestPropertySource(locations = "classpath:application-test.properties")
@SpringBatchTest
@Sql(statements = "delete from orders")
public class OrdersJobIT {

    @Autowired
    JobRepositoryTestUtils jobRepositoryTestUtils;

    @Autowired
    JobOperatorTestUtils jobOperatorTestUtils;

    @Autowired
    Job ordersJob;

    @Autowired
    JdbcClient jdbcClient;

    @BeforeEach
    void removeJobExecutions() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    void job() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("orders.date", "2026-06-08")
                .toJobParameters();
        jobOperatorTestUtils.setJob(ordersJob);
        JobExecution jobExecution = jobOperatorTestUtils.startJob(jobParameters);

        assertTrue(Files.exists(Path.of("staging/orders-2026-06-08.csv")));

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

        long count = (Long) jdbcClient.sql("select count(id) from orders")
                .query()
                .singleValue();
        assertEquals(18, count);
    }

    @Test
    void invalidDate() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("orders.date", "2026-06-09")
                .toJobParameters();
        jobOperatorTestUtils.setJob(ordersJob);
        JobExecution jobExecution = jobOperatorTestUtils.startJob(jobParameters);

        assertEquals(BatchStatus.FAILED, jobExecution.getStatus());
        assertEquals("FAILED", jobExecution.getExitStatus().getExitCode());
        assertTrue(jobExecution.getExitStatus().getExitDescription()
                .contains("NoSuchFileException"));
    }

    @Test
    void loadOrderStep() {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("orders.date", "2026-06-08")
                .toJobParameters();
        ExecutionContext executionContext = new ExecutionContext(
                Map.of("file", "input/orders-2026-06-08.csv")
        );
        JobExecution jobExecution = jobOperatorTestUtils.startStep(
                "loadOrdersStep",
                jobParameters,
                executionContext
        );

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());

        long count = (Long) jdbcClient.sql("select count(id) from orders")
                .query()
                .singleValue();
        assertEquals(18, count);

    }
}
