package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yycstart
 * @create 2020-12-11 10:46
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;
}
