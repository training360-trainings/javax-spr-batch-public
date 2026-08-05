package ordersbatch;

import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersValidator;

import java.time.LocalDate;

public class OrdersDateJobParametersValidator implements JobParametersValidator {

    @Override
    public void validate(JobParameters parameters) throws InvalidJobParametersException {
        String date = parameters.getString("orders.date");
        if (date == null) {
            throw new InvalidJobParametersException("orders.date is required");
        }
        try {
            LocalDate.parse(date);
        } catch (Exception e) {
            throw new InvalidJobParametersException("orders.date must be in the format yyyy-MM-dd");
        }
    }
}
