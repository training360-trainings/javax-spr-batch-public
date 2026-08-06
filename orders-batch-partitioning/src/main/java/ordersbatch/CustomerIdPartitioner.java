package ordersbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class CustomerIdPartitioner implements Partitioner {

    private final JdbcClient jdbcClient;

    private final LocalDate date;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        return jdbcClient.sql("""
                                WITH customers AS (
                                   SELECT DISTINCT customer_id
                                   FROM orders
                                   WHERE order_date = :date
                               )
                               SELECT
                                   grp,
                                   MIN(customer_id) AS min_customer_id,
                                   MAX(customer_id) AS max_customer_id,
                                   COUNT(*) AS customer_count
                               FROM (
                                   SELECT
                                       customer_id,
                                       NTILE(:partitions) OVER (ORDER BY customer_id) AS grp
                                   FROM customers
                               ) t
                               GROUP BY grp
                               ORDER BY grp
""")
                .param("date", date)
                .param("partitions", gridSize)
                .query((rs, rowNum) -> {
                    ExecutionContext context = new ExecutionContext();
                    context.putInt("partition", rowNum);
                    context.putInt("minCustomerId", rs.getInt("min_customer_id"));
                    context.putInt("maxCustomerId", rs.getInt("max_customer_id"));
                    return context;
                })
                .stream()
                .collect(
                        Collectors.toMap(
                                context -> "partition" + context.getInt("partition"),
                                context -> context
                        )
                );
    }
}
