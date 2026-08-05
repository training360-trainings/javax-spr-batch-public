package ordersbatch;

import org.springframework.batch.infrastructure.item.file.LineMapper;

public class OrderLineMapper implements LineMapper<OrderLine> {

    @Override
    public OrderLine mapLine(String line, int lineNumber) throws Exception {
        String[] fields = line.split(",");
        long customerId = Long.parseLong(fields[0]);
        long productId = Long.parseLong(fields[1]);
        String date = fields[2];
        int quantity = Integer.parseInt(fields[3]);
        return new OrderLine(lineNumber, customerId, productId, date, quantity);
    }
}
