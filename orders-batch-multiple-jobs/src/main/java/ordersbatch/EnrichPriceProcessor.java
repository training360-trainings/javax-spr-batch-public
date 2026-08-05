package ordersbatch;

import lombok.RequiredArgsConstructor;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

@RequiredArgsConstructor
public class EnrichPriceProcessor implements ItemProcessor<OrderEntity, OrderEntity> {

    private final PriceService priceService;

    @Override
    public @Nullable OrderEntity process(OrderEntity item) {
        long price = priceService.getPrice(item.getProductId());
        item.enrichPrice(price * item.getQuantity());
        return item;
    }
}
