package ordersbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class OrdersBatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrdersBatchApplication.class, args);
    }

}
