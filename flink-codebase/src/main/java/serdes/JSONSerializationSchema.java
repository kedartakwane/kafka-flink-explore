package serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import objects.Transaction;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JSONSerializationSchema implements SerializationSchema<Transaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(Transaction transaction) {
        try {
            return mapper.writeValueAsBytes(transaction);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
