package ordersbatch;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

public class OrderLineFilterProcessor implements ItemProcessor<OrderLine, OrderLine> {

    @Override
    public @Nullable OrderLine process(OrderLine item) {
        if (item.quantity() == 0) {
            return null;
        }
        return item;
    }
}
