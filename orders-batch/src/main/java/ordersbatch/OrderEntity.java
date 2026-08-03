package ordersbatch;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
@Setter
@Entity
@Table(name = "orders")
public class OrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    private long customerId;

    private long productId;

    @Column(name = "order_date")
    private LocalDate date;

    private int quantity;

    private long price;

    @Enumerated(EnumType.STRING)
    private PriceStatus priceStatus = PriceStatus.PENDING;

    public OrderEntity(long customerId, long productId, LocalDate date, int quantity) {
        this.customerId = customerId;
        this.productId = productId;
        this.date = date;
        this.quantity = quantity;
    }

    public void enrichPrice(long price) {
        this.price = price;
        this.priceStatus = PriceStatus.ENRICHED;
    }
}
