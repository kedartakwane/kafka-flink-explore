package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import objects.Customer;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class CustomerJSONDeserializationSchema implements DeserializationSchema<Customer> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Customer deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Customer.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<Customer> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(Customer transaction) {
        return false;
    }

    @Override
    public TypeInformation<Customer> getProducedType() {
        return TypeInformation.of(Customer.class);
    }
}
