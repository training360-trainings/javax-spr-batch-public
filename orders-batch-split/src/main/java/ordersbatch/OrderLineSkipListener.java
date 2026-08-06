package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.nio.file.Path;

@Slf4j
@RequiredArgsConstructor
public class OrderLineSkipListener implements SkipListener<OrderLine, OrderEntity>,
        StepExecutionListener {

    private FlatFileItemWriter<ErrorRecord> writer;

    private final Path stagingDir;

    private final String date;

    @Override
    @SneakyThrows
    public void onSkipInProcess(OrderLine item, Throwable t) {
        log.info("Skipping order line: {}, {}", item.rowNum(), t.getMessage());
        writer.write(new Chunk<>(new ErrorRecord(item.rowNum(), t.getMessage())));
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        BeanWrapperFieldExtractor<ErrorRecord> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"rowNum", "errorMessage"});
        DelimitedLineAggregator<ErrorRecord> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FileSystemResource file = new FileSystemResource(stagingDir.resolve("errors-%s.csv".formatted(date)));
        writer = new FlatFileItemWriter<>(file, aggregator);
        writer.open(stepExecution.getExecutionContext());
    }

    @Override
    public @Nullable ExitStatus afterStep(StepExecution stepExecution) {
        writer.close();
        return null;
    }
}
