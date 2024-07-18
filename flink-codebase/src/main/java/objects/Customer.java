package objects;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class Customer {
    private String customerId;
    private String name;
    private String email;
    private String birthdate;
}
