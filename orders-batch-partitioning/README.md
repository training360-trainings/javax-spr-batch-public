# Orders Batch Application

```
└── ordersJob
    ├── copyFileStep
    │   └── copyFileTasklet
    ├── loadOrdersManagerStep
    │   ├── loadOrdersPartitioner:MultiResourcePartitioner
    │   └── loadOrdersPartitionHandler:TaskExecutorPartitionHandler
    │       └── loadOrdersStep
    │           ├── orderFileReader: FlatFileItemReader
    │           ├── :CompositeItemProcessorBuilder
    │           │   ├── orderLineFilterProcessor: custom
    │           │   ├── orderLineValidatorProcessor: BeanValidatingItemProcessor
    │           │   └── orderLineToOrderEntityProcessor: custom
    │           └── orderWriter: RepositoryItemWriter
    └── enrichmentStep
        ├── orderReader: RepositoryItemReader
        ├── enrichPriceProcessor: custom
        └── orderWriter: RepositoryItemWriter
```
