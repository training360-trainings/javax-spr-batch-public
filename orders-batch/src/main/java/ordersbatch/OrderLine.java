package ordersbatch;

public record OrderLine(long rowNum, long customerId, long productId, String date, int quantity) {
}
