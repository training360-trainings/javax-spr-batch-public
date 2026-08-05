# Orders Batch Application

```
└── ordersJob
    ├── copyFileStep
    │   └── copyFileTasklet
    ├── loadOrdersStep
    │   ├── orderFileReader: FlatFileItemReader
    │   ├── :CompositeItemProcessorBuilder
    │   │   ├── orderLineFilterProcessor: custom
    │   │   ├── orderLineValidatorProcessor: custom
    │   │   └── orderLineToOrderEntityProcessor: custom
    │   └── orderWriter: RepositoryItemWriter
    ├── enrichmentStep
    │   ├── orderReader: RepositoryItemReader
    │   ├── enrichPriceProcessor: custom
    │   └── orderWriter: RepositoryItemWriter
    └── callLogJobStep
        └── logJob: Job
```
