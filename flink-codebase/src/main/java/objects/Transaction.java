package objects;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Transaction {
    private String receiptId;
    private String customerId;
    private String productId;
    private String productName;
    private double productPrice;
    private int productQuantity;
    private double totalAmount;
    private Timestamp receiptDate;
    private String paymentMethod;
}
