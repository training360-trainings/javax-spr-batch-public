package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@RequiredArgsConstructor
public class CopyFileTasklet implements Tasklet
    {
        private final String date;

        private final Path inputDir;

        private final Path stagingDir;

        @Override
        public @Nullable RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
            log.info("Copying file");

//            JobParameters parameters = contribution.getStepExecution()
//                    .getJobExecution()
//                    .getJobParameters();
//            String date = parameters.getString("orders.date");

            FileSystemUtils.deleteRecursively(stagingDir);
            Files.createDirectories(stagingDir);

//            String fileName = "orders-" + date + ".csv";
//            Path inputFile = inputDir.resolve(fileName);
//            Path outputPath = stagingDir.resolve(fileName);
//
//            Files.copy(inputFile, outputPath);
//
//            chunkContext
//                    .getStepContext()
//                    .getStepExecution()
//                    .getJobExecution()
//                    .getExecutionContext()
//                    .put("file", outputPath.toString());

            Files.list(inputDir)
                    .filter(p -> p.getFileName().toString().startsWith("orders-" + date))
                    .forEach(file -> {
                        try {
                            Files.copy(file, stagingDir.resolve(file.getFileName()));
                        } catch (IOException ioe) {
                            throw new IllegalStateException("Can not copy file", ioe);
                        }
                    });

//            throw new IllegalStateException("Simulated error");
            return RepeatStatus.FINISHED;
        }
    }
