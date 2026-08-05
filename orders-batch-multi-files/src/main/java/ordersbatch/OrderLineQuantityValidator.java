package ordersbatch;

import org.springframework.batch.infrastructure.item.validator.ValidationException;
import org.springframework.batch.infrastructure.item.validator.Validator;

public class OrderLineQuantityValidator implements Validator<OrderLine> {

    @Override
    public void validate(OrderLine value) throws ValidationException {
        if (value.quantity() < 0) {
            throw new ValidationException("In row %d invalid quantity: %d".formatted(
                    value.rowNum(), value.quantity()));
        }
    }
}
