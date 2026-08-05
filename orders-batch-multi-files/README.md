# Orders Batch Application

```
└── ordersJob
    ├── copyFileStep
    │   └── copyFileTasklet
    ├── loadOrdersStep
    │   ├── orderMultiResourceReader:MultiResourceItemReader
    │   │   └── orderFileReader: FlatFileItemReader
    │   ├── :CompositeItemProcessorBuilder
    │   │   ├── orderLineFilterProcessor: custom
    │   │   ├── orderLineValidatorProcessor: BeanValidatingItemProcessor
    │   │   └── orderLineToOrderEntityProcessor: custom
    │   └── orderWriter: RepositoryItemWriter
    ├── enrichmentStep
    │   ├── orderReader: RepositoryItemReader
    │   ├── enrichPriceProcessor: custom
    │   └── orderWriter: RepositoryItemWriter
    └── billsStep
        ├── customerIdReader: JdbcPagingItemReader
        ├── createBillForCustomerProcessor: custom
        └── billFileWriter: FlatFileItemWriter
```
