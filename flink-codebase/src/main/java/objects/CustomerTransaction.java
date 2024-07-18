package objects;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class CustomerTransaction implements Serializable {
    private String receiptId;
    private String customerId;
    private String productId;
    private String productName;
    private double productPrice;
    private int productQuantity;
    private double totalAmount;
    private Timestamp receiptDate;
    private String paymentMethod;
    private String customerName;
    private String customerEmail;
    private String customerBirthdate;

    public CustomerTransaction() {

    }

    public CustomerTransaction(Customer customer, Transaction transaction) {
        setCustomerId(customer.getCustomerId());
        setReceiptId(transaction.getReceiptId());
        setProductId(transaction.getProductId());
        setProductName(transaction.getProductName());
        setProductPrice(transaction.getProductPrice());
        setProductQuantity(transaction.getProductQuantity());
        setTotalAmount(transaction.getTotalAmount());
        setReceiptDate(transaction.getReceiptDate());
        setPaymentMethod(transaction.getPaymentMethod());
        setCustomerName(customer.getName());
        setCustomerEmail(customer.getEmail());
        setCustomerBirthdate(customer.getBirthdate());
    }
}
