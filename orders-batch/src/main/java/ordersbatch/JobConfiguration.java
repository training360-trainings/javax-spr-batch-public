package ordersbatch;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.data.RepositoryItemReader;
import org.springframework.batch.infrastructure.item.data.RepositoryItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JpaCursorItemReader;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.database.JpaPagingItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
@Slf4j
@EnableConfigurationProperties(OrdersProperties.class)
public class JobConfiguration {

    private final JobRepository jobRepository;

    private final PlatformTransactionManager platformTransactionManager;

    @Bean
    public Job ordersJob(Step copyFileStep,
//                         Step echoStep,
                         Step loadOrdersStep,
                         Step enrichmentStep,
                         OrdersDateJobParametersValidator ordersDateJobParametersValidator) {
        return new JobBuilder("ordersJob", jobRepository)
                .start(copyFileStep)
//                .next(echoStep)
                .next(loadOrdersStep)
                .next(enrichmentStep)
                .validator(ordersDateJobParametersValidator)
                .build();
    }

    @Bean
    public Step copyFileStep(Tasklet copyFileTasklet) {
        return new StepBuilder("copyFileStep", jobRepository)
                .tasklet(copyFileTasklet)
                .build();
    }

    @Bean Step echoStep(Tasklet echoTasklet) {
        return new StepBuilder("echoStep", jobRepository)
                .tasklet(echoTasklet)
                .build();
    }

    @Bean
    public Step loadOrdersStep(FlatFileItemReader<OrderLine> orderFileReader,
                               OrderLineFilterProcessor orderLineFilterProcessor,
                               OrderLineToOrderEntityProcessor orderLineToOrderEntityProcessor,
//                               EnrichPriceProcessor enrichPriceProcessor,
                               ConverterItemListener converterItemListener,
                               ItemWriter<OrderEntity> orderWriter) {
        return new StepBuilder("loadOrdersStep", jobRepository)
                .<OrderLine, OrderEntity>chunk(10)
                .transactionManager(platformTransactionManager)
                .reader(orderFileReader)
                .processor(
                        new CompositeItemProcessorBuilder<OrderLine, OrderEntity>()
                                .delegates(
                                        orderLineFilterProcessor,
                                        orderLineToOrderEntityProcessor //,
//                                        enrichPriceProcessor
                                ).build()
                )
                .listener(converterItemListener)
                .writer(orderWriter)
                .build();
    }

    @Bean
    public Step enrichmentStep(RepositoryItemReader<OrderEntity> orderReader,
                               EnrichPriceProcessor enrichPriceProcessor,
                               ItemWriter<OrderEntity> orderWriter) {
        return new StepBuilder("enrichmentStep", jobRepository)
                .<OrderEntity, OrderEntity>chunk(10)
                .transactionManager(platformTransactionManager)
                .reader(orderReader)
                .processor(enrichPriceProcessor)
                .writer(orderWriter)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet copyFileTasklet(@Value("#{jobParameters['orders.date']}") String date,
                                   OrdersProperties ordersProperties) {
        return new CopyFileTasklet(date, ordersProperties.inputDir(), ordersProperties.stagingDir());
    }

    @Bean
    public Tasklet echoTasklet() {
        SystemCommandTasklet tasklet = new SystemCommandTasklet();
        tasklet.setCommand("cmd", "/c", "dir", ">", "dir.txt");
        tasklet.setTimeout(Duration.ofSeconds(5).toMillis());
        return tasklet;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<OrderLine> orderFileReader(OrdersProperties ordersProperties,
                                                         OrderLineMapper orderLineMapper,
                                                         @Value("#{jobExecutionContext['file']}") String file) {
        return new FlatFileItemReaderBuilder<OrderLine>()
                .name("orderFileReader")
//                .resource(new FileSystemResource(ordersProperties.stagingDir().resolve("orders-2026-06-08.csv")))
                .resource(new FileSystemResource(file))
                .linesToSkip(1)
//                .delimited()
//                .names("customerId", "productId", "date", "quantity")
//                .targetType(OrderLine.class)
                .lineMapper(orderLineMapper)
                .build();
    }

    @Bean
    public OrderLineMapper orderLineMapper() {
        return new OrderLineMapper();
    }

    @Bean
    public OrderLineFilterProcessor orderLineFilterProcessor() {
        return new OrderLineFilterProcessor();
    }

    @Bean
    public OrderLineToOrderEntityProcessor orderLineToOrderEntityProcessor() {
        return new OrderLineToOrderEntityProcessor();
    }

    @Bean
    public EnrichPriceProcessor enrichPriceProcessor(PriceService priceService) {
        return new EnrichPriceProcessor(priceService);
    }

    @Bean
    public ConverterItemListener converterItemListener() {
        return new ConverterItemListener();
    }

//    @Bean
//    public LoggingItemWriter<OrderEntity> orderWriter() {
//        return new LoggingItemWriter<>();
//    }

//    @Bean
//    public JdbcBatchItemWriter<OrderEntity> orderWriter(DataSource dataSource) {
//        String sql = "insert into orders (customer_id, product_id, order_date, quantity) values (:customerId, :productId, :date, :quantity)";
//        return new JdbcBatchItemWriterBuilder<OrderEntity>()
//                .dataSource(dataSource)
//                .sql(sql)
//                .beanMapped()
//                .build();
//    }

//    @Bean
//    public JpaItemWriter<OrderEntity> orderWriter(EntityManagerFactory entityManagerFactory) {
//        return new JpaItemWriter<>(entityManagerFactory);
//    }

    @Bean
    public ItemWriter<OrderEntity> orderWriter(OrderRepository orderRepository) {
        return new RepositoryItemWriter<>(orderRepository);
    }

    @Bean
    public OrdersDateJobParametersValidator ordersDateJobParametersValidator() {
        return new OrdersDateJobParametersValidator();
    }

//    @Bean
//    @StepScope
//    public JpaCursorItemReader<OrderEntity> orderReader(EntityManagerFactory entityManagerFactory,
//                                                        @Value("#{jobParameters['orders.date']}") String date) {
//        JpaCursorItemReader<OrderEntity> reader = new JpaCursorItemReader<>(entityManagerFactory);
//        reader.setQueryString("select o from OrderEntity o where o.date = :date");
//        reader.setParameterValues(Map.of("date", LocalDate.parse(date)));
//        return reader;
//    }

//    @Bean
//    @StepScope
//    public JpaPagingItemReader<OrderEntity> orderReader(EntityManagerFactory entityManagerFactory,
//                                                                   @Value("#{jobParameters['orders.date']}") String date) {
//        JpaPagingItemReader<OrderEntity> reader = new JpaPagingItemReader<>(entityManagerFactory);
//        reader.setQueryString("select o from OrderEntity o where o.date = :date");
//        reader.setParameterValues(Map.of("date", LocalDate.parse(date)));
//        reader.setPageSize(10);
//        return reader;
//    }

    @Bean
    @StepScope
    public RepositoryItemReader<OrderEntity> orderReader(OrderRepository orderRepository,
                                                        @Value("#{jobParameters['orders.date']}") String date) {
        RepositoryItemReader<OrderEntity> reader =
                new RepositoryItemReader<>(orderRepository, Map.of("id",
                        Sort.Direction.ASC));
        reader.setMethodName("findOrderEntitiesByDate");
        reader.setArguments(List.of(LocalDate.parse(date)));
        reader.setPageSize(10);
        return reader;

    }
}
