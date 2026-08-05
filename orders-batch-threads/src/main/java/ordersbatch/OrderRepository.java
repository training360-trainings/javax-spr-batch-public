package ordersbatch;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;

public interface OrderRepository extends JpaRepository<OrderEntity, Long> {

    Page<OrderEntity> findOrderEntitiesByDate(LocalDate date, Pageable pageable);
}
