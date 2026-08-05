package ordersbatch;

import java.time.LocalDate;
import java.util.List;

public record Bill(long customerId, LocalDate date, List<BillItem> items) {

    public long getPrice() {
        return items.stream().mapToLong(BillItem::price).sum();
    }
}
