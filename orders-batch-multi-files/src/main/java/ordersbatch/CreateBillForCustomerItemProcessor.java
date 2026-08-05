package ordersbatch;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.jdbc.core.simple.JdbcClient;

import java.time.LocalDate;
import java.util.List;

@RequiredArgsConstructor
public class CreateBillForCustomerItemProcessor implements
        ItemProcessor<Long, Bill> {

    private final JdbcClient jdbcClient;

    private final LocalDate date;

    @Override
    public @Nullable Bill process(Long item) throws Exception {
        List<BillItem> billItems =
                jdbcClient
                        .sql("""
                        select product_id, quantity, price from orders 
                                                        where customer_id = :customerId and
                                                              order_date = :date
                        """)
                        .param("customerId", item)
                        .param("date", date)
                        .query(BillItem.class)
                        .list()
                ;
        return new Bill(item, date, billItems);
    }
}
