package ordersbatch;

import lombok.Getter;
import org.springframework.batch.core.listener.ItemWriteListener;
import org.springframework.batch.infrastructure.item.Chunk;

public class BillItemWriteListener implements ItemWriteListener<Bill> {

    @Getter
    private long sum;

    @Override
    public void afterWrite(Chunk<? extends Bill> items) {
        for (Bill bill : items) {
           sum += bill.getPrice();
        }
    }
}
