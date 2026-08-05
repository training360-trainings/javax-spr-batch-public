package ordersbatch;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;

@Configuration(proxyBeanMethods = false)
public class BatchConfiguration {

    @Bean
    public AsyncTaskExecutor batchTaskExecutor() {
        return new VirtualThreadTaskExecutor();
    }
}
