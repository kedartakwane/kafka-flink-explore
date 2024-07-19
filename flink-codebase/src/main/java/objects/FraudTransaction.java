package objects;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class FraudTransaction {
    private String receiptId;
    private Timestamp detectedTs;
}
