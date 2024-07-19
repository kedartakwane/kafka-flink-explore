package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import objects.FraudTransaction;
import objects.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class FraudTransactionJSONDeserializationSchema implements DeserializationSchema<FraudTransaction> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public FraudTransaction deserialize(byte[] bytes) throws IOException {
        return mapper.readValue(bytes, FraudTransaction.class);
    }

    @Override
    public void deserialize(byte[] message, Collector<FraudTransaction> out) throws IOException {
        DeserializationSchema.super.deserialize(message, out);
    }

    @Override
    public boolean isEndOfStream(FraudTransaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<FraudTransaction> getProducedType() {
        return TypeInformation.of(FraudTransaction.class);
    }
}
