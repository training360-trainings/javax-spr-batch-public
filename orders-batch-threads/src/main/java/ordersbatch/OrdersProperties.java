package ordersbatch;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;

@ConfigurationProperties(prefix = "orders")
public record OrdersProperties(Path inputDir, Path stagingDir) {
}
