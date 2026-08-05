package ordersbatch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;

import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.Locale;
import java.util.Map;

@RequiredArgsConstructor
public class BillWriter implements ItemWriter<Bill> {

    private final SpringTemplateEngine templateEngine;

    private final Path stagingDir;

    private final LocalDate date;


    @Override
    public void write(Chunk<? extends Bill> chunk) throws Exception {
        for (Bill bill : chunk.getItems()) {
            Path file = stagingDir.resolve("bill-%s-%d.html".formatted(date, bill.customerId()));
            try (

                    Writer writer = Files.newBufferedWriter(file);
                    ) {
                templateEngine.process("bill", new Context(Locale.ENGLISH,
                        Map.of("bill", bill)), writer);
            }
        }
    }
}
