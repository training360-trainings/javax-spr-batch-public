package ordersbatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;

@Slf4j
public class LoggingItemWriter<T> implements ItemWriter<T> {

    @Override
    public void write(Chunk<? extends T> chunk) throws Exception {
        log.info("Writing chunk: {}", chunk.getItems());
    }
}
