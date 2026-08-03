package ordersbatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PriceService {

    @Cacheable("prices")
    public long getPrice(long productId) {
        log.info("Getting price for product {}", productId);
        return 100;
    }
}
