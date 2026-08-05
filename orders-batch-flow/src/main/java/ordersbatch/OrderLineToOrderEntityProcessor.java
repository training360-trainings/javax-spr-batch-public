package ordersbatch;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

import java.time.Duration;
import java.time.LocalDate;

@Slf4j
@RequiredArgsConstructor
public class OrderLineToOrderEntityProcessor implements ItemProcessor<OrderLine, OrderEntity> {

    private final LocalDate jobParametersDate;

    @Override
    public @Nullable OrderEntity process(OrderLine item) throws Exception {
        log.info("Processing order line: {}", item);

//        log.info("Sleeping for 5 seconds");
//        Thread.sleep(Duration.ofSeconds(5));

        LocalDate date;
        try {
            date = LocalDate.parse(item.date());
        } catch (Exception e) {
            throw new IllegalArgumentException("In row %d invalid date: %s".formatted(
                    item.rowNum(), item.date()));
        }

        if (!date.equals(jobParametersDate)) {
            throw new IllegalArgumentException("In row %d date %s does not match job parameter date %s".formatted(
                    item.rowNum(), item.date(), jobParametersDate));
        }

//        if (item.quantity() < 0) {
//            throw new IllegalArgumentException("In row %d invalid quantity: %d".formatted(
//                    item.rowNum(), item.quantity()));
//        }

        return new OrderEntity(
                item.customerId(),
                item.productId(),
                date,
                item.quantity()
        );
    }
}
