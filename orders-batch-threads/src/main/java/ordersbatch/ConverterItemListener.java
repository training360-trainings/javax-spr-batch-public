package ordersbatch;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.listener.ItemProcessListener;

@Slf4j
public class ConverterItemListener
//        implements ItemProcessListener<OrderLine, OrderEntity>
{

//    @Override
    @AfterProcess
    public void afterProcess(OrderLine item, @Nullable OrderEntity result) {
        log.info("Processed order line in listener: {}", item);
    }
}
