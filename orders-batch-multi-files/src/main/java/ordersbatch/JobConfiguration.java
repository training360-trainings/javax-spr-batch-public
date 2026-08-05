package ordersbatch;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.data.RepositoryItemReader;
import org.springframework.batch.infrastructure.item.data.RepositoryItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JpaCursorItemReader;
import org.springframework.batch.infrastructure.item.database.JpaItemWriter;
import org.springframework.batch.infrastructure.item.database.JpaPagingItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.infrastructure.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.MultiResourceItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.builder.MultiResourceItemReaderBuilder;
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
import org.springframework.data.domain.Sort;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.thymeleaf.spring6.SpringTemplateEngine;

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
                         Step loadOrdersStep,
                         Step enrichmentStep,
                         Step billsStep,
                         OrdersDateJobParametersValidator ordersDateJobParametersValidator) {
        return new JobBuilder("ordersJob", jobRepository)
                .start(copyFileStep)
//                .next(echoStep)
                .next(loadOrdersStep)
                .next(enrichmentStep)
                .next(billsStep)
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
    public Step loadOrdersStep(
            //FlatFileItemReader<OrderLine> orderFileReader,
            MultiResourceItemReader<OrderLine> orderLineMultiResourceReader,
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
                .reader(orderLineMultiResourceReader)
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
    public Step billsStep(
            ItemReader<Long> customerIdItemReader,
            ItemProcessor<Long, Bill> createBillForCustomerItemProcessor,
            ItemWriter<Bill> billWriter,
            BillItemWriteListener billItemWriteListener
    ) {
        return new StepBuilder("billsStep", jobRepository)
                .<Long, Bill>chunk(1)
                .transactionManager(platformTransactionManager)
                .reader(customerIdItemReader)
                .processor(createBillForCustomerItemProcessor)
                .writer(billWriter)
                .listener(billItemWriteListener)
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
    public MultiResourceItemReader<OrderLine> orderLineMultiResourceReader(
            OrdersProperties ordersProperties,
            FlatFileItemReader<OrderLine> orderFileReader
    ) {
        List<FileSystemResource> resources = Files.list(ordersProperties.stagingDir())
                .map(FileSystemResource::new)
                .toList();

        return new MultiResourceItemReaderBuilder<OrderLine>()
                .name("orderLineMultiResourceReader")
                .delegate(orderFileReader)
                .resources(resources.toArray(Resource[]::new))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<OrderLine> orderFileReader(OrdersProperties ordersProperties,
                                                         OrderLineMapper orderLineMapper,
                                                         @Value("#{jobExecutionContext['file']}") String file) {
        return new FlatFileItemReaderBuilder<OrderLine>()
                .name("orderFileReader")
//                .resource(new FileSystemResource(ordersProperties.stagingDir().resolve("orders-2026-06-08.csv")))
//                .resource(new FileSystemResource(file))
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
                                                        @Value("#{jobParameters['orders.date']}") String date) {
        RepositoryItemReader<OrderEntity> reader =
                new RepositoryItemReader<>(orderRepository, Map.of("id",
                        Sort.Direction.ASC));
        reader.setMethodName("findOrderEntitiesByDate");
        reader.setArguments(List.of(LocalDate.parse(date)));
        reader.setPageSize(10);
        return reader;

    }

    @Bean
    @SneakyThrows
    @StepScope
    public ItemReader<Long> customerIdItemReader(
            DataSource dataSource,
            @Value("#{jobParameters['orders.date']}") String date
    ) {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();
        queryProvider.setSelectClause("select distinct customer_id");
        queryProvider.setFromClause("from orders");
        queryProvider.setWhereClause("where order_date = :date");
        queryProvider.setSortKey("customer_id");
        queryProvider.setDataSource(dataSource);

        return new JdbcPagingItemReaderBuilder<Long>()
                .name("customerIdItemReader")
                .dataSource(dataSource)
                .queryProvider(queryProvider.getObject())
                .parameterValues(Map.of("date", LocalDate.parse(date)))
                .rowMapper((rs, rowNum) -> rs.getLong("customer_id"))
                .pageSize(1)
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<Long, Bill> createBillForCustomerItemProcessor(
            JdbcClient jdbcClient,
            @Value("#{jobParameters['orders.date']}") String date
    ) {
        return new CreateBillForCustomerItemProcessor(jdbcClient, LocalDate.parse(date));
    }

//    @Bean
//    @StepScope
//    public FlatFileItemWriter<Bill> billWriter(
//            OrdersProperties ordersProperties,
//            @Value("#{jobParameters['orders.date']}") String date,
//            BillItemWriteListener billItemWriteListener
//    ) {
//        FileSystemResource resource = new FileSystemResource(
//                ordersProperties.stagingDir().resolve("bills-" + date + ".csv")
//        );
//
//        return new FlatFileItemWriterBuilder<Bill>()
//                .name("billWriter")
//                .resource(resource)
////                .delimited(config -> config.delimiter(",").names("customerId", "date"))
////                .formatted(config -> config.format("%s - %s").names("customerId", "date"))
//                .lineAggregator(Record::toString)
//                .headerCallback(writer -> writer.write("Bills"))
//                .footerCallback(writer -> writer.write("Sum: " + billItemWriteListener.getSum()))
//                .build();
//    }

    @Bean
    @StepScope
    public ItemWriter<Bill> billWriter(
            SpringTemplateEngine templateEngine,
            OrdersProperties ordersProperties,
            @Value("#{jobParameters['orders.date']}") String date
    ) {
        return new BillWriter(templateEngine, ordersProperties.stagingDir(),
                LocalDate.parse(date));
    }

    @Bean
    @StepScope
    public BillItemWriteListener billItemWriteListener() {
        return new BillItemWriteListener();
    }
}
