package ordersbatch;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ItemProcessor;
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
import org.springframework.batch.infrastructure.item.validator.BeanValidatingItemProcessor;
import org.springframework.batch.infrastructure.item.validator.ValidatingItemProcessor;
import org.springframework.batch.infrastructure.item.validator.ValidationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;

import javax.sql.DataSource;
import java.nio.file.Files;
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
//                         Step loadOrdersStep,
                         Step loadOrdersManagerStep,
//                         Step enrichmentStep,
                         Step enrichmentManagerStep,
                         OrdersDateJobParametersValidator ordersDateJobParametersValidator) {
        return new JobBuilder("ordersJob", jobRepository)
                .start(copyFileStep)
//                .next(echoStep)
//                .next(loadOrdersStep)
                .next(loadOrdersManagerStep)
//                .next(enrichmentStep)
                .next(enrichmentManagerStep)
                .validator(ordersDateJobParametersValidator)
                .build();
    }

    @Bean
    public Step copyFileStep(Tasklet copyFileTasklet) {
        return new StepBuilder("copyFileStep", jobRepository)
                .tasklet(copyFileTasklet)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean Step echoStep(Tasklet echoTasklet) {
        return new StepBuilder("echoStep", jobRepository)
                .tasklet(echoTasklet)
                .build();
    }

    @Bean
    public Step loadOrdersManagerStep(MultiResourcePartitioner loadOrdersPartitioner,
                                      PartitionHandler loadOrdersPartitionHandler) {
        return new StepBuilder("loadOrdersManagerStep", jobRepository)
                .partitioner("loadOrdersStep", loadOrdersPartitioner)
                .partitionHandler(loadOrdersPartitionHandler)
                .build();
    }

    @Bean
    public Step loadOrdersStep(FlatFileItemReader<OrderLine> orderFileReader,
                               OrderLineFilterProcessor orderLineFilterProcessor,
                               ItemProcessor<OrderLine, OrderLine> orderLineValidatorProcessor,
                               OrderLineToOrderEntityProcessor orderLineToOrderEntityProcessor,
//                               EnrichPriceProcessor enrichPriceProcessor,
                               ConverterItemListener converterItemListener,
                               OrderLineSkipListener orderLineSkipListener,
                               ItemWriter<OrderEntity> orderWriter) {
        DefaultTransactionAttribute transactionAttribute = new DefaultTransactionAttribute();
        transactionAttribute.setIsolationLevel(TransactionAttribute.ISOLATION_DEFAULT);
        transactionAttribute.setTimeout((int) Duration.ofSeconds(5).toSeconds());

        return new StepBuilder("loadOrdersStep", jobRepository)
                .<OrderLine, OrderEntity>chunk(10)
                .transactionManager(platformTransactionManager)
                .transactionAttribute(transactionAttribute)
                .reader(orderFileReader)
                .processor(
                        new CompositeItemProcessorBuilder<OrderLine, OrderEntity>()
                                .delegates(
                                        orderLineFilterProcessor,
                                        orderLineValidatorProcessor,
                                        orderLineToOrderEntityProcessor //,
//                                        enrichPriceProcessor
                                ).build()
                )
                .listener(converterItemListener)
                .listener(orderLineSkipListener)
                .writer(orderWriter)
                .startLimit(4)
                .faultTolerant()
                .skip(ValidationException.class)
                .skipLimit(2)
                .build();
    }

    @Bean
    public Step enrichmentManagerStep(
            CustomerIdPartitioner customerIdPartitioner,
            PartitionHandler enrichmentPartitionHandler
    ) {
        return new StepBuilder("enrichmentManagerStep", jobRepository)
                .partitioner("enrichmentStep", customerIdPartitioner)
                .partitionHandler(enrichmentPartitionHandler)
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
                .faultTolerant()
//                .retry(IllegalStateException.class)
//                .retryLimit(5)
                .retryPolicy(
                        RetryPolicy
                                .builder()
                                .includes(IllegalStateException.class)
                                .maxRetries(5)
                                .delay(Duration.ofMillis(10))
                                .build()
                )
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
    @SneakyThrows
    @StepScope
    public MultiResourcePartitioner loadOrdersPartitioner(
            OrdersProperties ordersProperties
    ) {
        List<FileSystemResource> resources =
                Files.list(ordersProperties.stagingDir())
                        .map(FileSystemResource::new)
                        .toList();

        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setResources(resources.toArray(Resource[]::new));
        partitioner.setKeyName("file");
        return partitioner;
    }

    @Bean
    public PartitionHandler loadOrdersPartitionHandler(Step loadOrdersStep,
                                                       AsyncTaskExecutor batchTaskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(loadOrdersStep);
        handler.setTaskExecutor(batchTaskExecutor);
        handler.setGridSize(2);
        return handler;
    }

    @Bean
    @StepScope
    public FlatFileItemReader<OrderLine> orderFileReader(OrdersProperties ordersProperties,
                                                         OrderLineMapper orderLineMapper,
                                                         @Value("#{stepExecutionContext['file']}") Resource file) {
        return new FlatFileItemReaderBuilder<OrderLine>()
                .name("orderFileReader")
//                .resource(new FileSystemResource(ordersProperties.stagingDir().resolve("orders-2026-06-08.csv")))
//                .resource(new FileSystemResource(file))
                .resource(file)
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
    public ItemProcessor<OrderLine, OrderLine> orderLineValidatorProcessor() {
        return new BeanValidatingItemProcessor<>();
    }

//    @Bean
//    public ItemProcessor<OrderLine, OrderLine> orderLineValidatorProcessor(OrderLineQuantityValidator orderLineQuantityValidator) {
//        return new ValidatingItemProcessor<>(orderLineQuantityValidator);
//    }
//
//    @Bean
//    public OrderLineQuantityValidator orderLineQuantityValidator() {
//        return new OrderLineQuantityValidator();
//    }

    @Bean
    @StepScope
    public OrderLineToOrderEntityProcessor orderLineToOrderEntityProcessor(@Value("#{jobParameters['orders.date']}") String date) {
        return new OrderLineToOrderEntityProcessor(LocalDate.parse(date));
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
    @StepScope
    public OrderLineSkipListener orderLineSkipListener(OrdersProperties ordersProperties,
                                                       @Value("#{jobParameters['orders.date']}") String date) {
        return new OrderLineSkipListener(ordersProperties.stagingDir(), date);
    }

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
                                                        @Value("#{jobParameters['orders.date']}") String date,
                                                         @Value("#{stepExecutionContext['minCustomerId']}") long minCustomerId,
                                                         @Value("#{stepExecutionContext['maxCustomerId']}") long maxCustomerId
                                                         ) {
        RepositoryItemReader<OrderEntity> reader =
                new RepositoryItemReader<>(orderRepository, Map.of("id",
                        Sort.Direction.ASC));
//        reader.setMethodName("findOrderEntitiesByDate");
        reader.setMethodName("findOrderEntitiesByDateAndCustomerIdIsBetween");
//        reader.setArguments(List.of(LocalDate.parse(date)));

        reader.setArguments(List.of(LocalDate.parse(date),
                minCustomerId,
                maxCustomerId));
        reader.setPageSize(10);
        return reader;

    }

    @Bean
    @StepScope
    public CustomerIdPartitioner customerIdPartitioner(
            JdbcClient jdbcClient,
            @Value("#{jobParameters['orders.date']}") String date
    ) {
        return new CustomerIdPartitioner(jdbcClient, LocalDate.parse(date));
    }

    @Bean
    public PartitionHandler enrichmentPartitionHandler(
            Step enrichmentStep,
            AsyncTaskExecutor batchTaskExecutor
    ) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(enrichmentStep);
        handler.setTaskExecutor(batchTaskExecutor);
        handler.setGridSize(2);
        return handler;
    }
}
