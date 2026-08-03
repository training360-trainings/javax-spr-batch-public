package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import java.time.LocalDate;

@Slf4j
@RequiredArgsConstructor
public class OrderLineToOrderEntityProcessor implements ItemProcessor<OrderLine, OrderEntity> {

    @Override
    public @Nullable OrderEntity process(OrderLine item) throws Exception {
        log.info("Processing order line: {}", item);
        return new OrderEntity(
                item.customerId(),
                item.productId(),
                LocalDate.parse(item.date()),
                item.quantity()
        );
    }
}
