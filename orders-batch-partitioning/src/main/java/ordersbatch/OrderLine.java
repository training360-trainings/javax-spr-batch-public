package ordersbatch;

import jakarta.validation.constraints.Min;

public record OrderLine(long rowNum, long customerId, long productId, String date, @Min(0) int quantity) {
}
