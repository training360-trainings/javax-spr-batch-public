package ordersbatch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
@Slf4j
public class PriceService {

    private Random random = new Random();

    @Cacheable("prices")
    public long getPrice(long productId) {
        log.info("Getting price for product {}", productId);

        if (random.nextBoolean()) {
            throw new IllegalStateException("Simulated error");
        }

        return 100;
    }
}
