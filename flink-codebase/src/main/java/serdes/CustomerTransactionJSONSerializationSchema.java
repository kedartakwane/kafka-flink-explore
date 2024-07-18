package serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.CustomerTransaction;
import objects.Transaction;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class CustomerTransactionJSONSerializationSchema implements SerializationSchema<CustomerTransaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(CustomerTransaction obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
