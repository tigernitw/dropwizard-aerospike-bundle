package io.dropwizard.aerospike.connectivity;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.aerospike.config.AerospikeConfiguration;

import java.util.Optional;

public class AerospikeCommands {

    private final AerospikeConnection aerospikeConnection;
    private final ObjectMapper objectMapper;

    private static final String DEFAULT_BIN = "data";

    public AerospikeCommands(final AerospikeConnection aerospikeConnection,
                             final ObjectMapper objectMapper) {
        this.aerospikeConnection = aerospikeConnection;
        this.objectMapper = objectMapper;
    }

    public <T> void save(final String storeName, final String storeKey, final T storeValue, final int expirySeconds, final RecordExistsAction recordExistsAction) throws Exception {
        final WritePolicy writePolicy = getWritePolicyPolicy();
        final Key key = getKey(storeName, storeKey);
        final Bin bin = getBin(storeValue);

        writePolicy.recordExistsAction = recordExistsAction;
        if (expirySeconds > 0) {
            writePolicy.expiration = expirySeconds;
        }
        aerospikeConnection.getClient().put(writePolicy, key, bin);
    }

    public <T> void saveOrReplace(final String storeName, final String storeKey, final T storeValue, final int expirySeconds) throws Exception {
        save(storeName, storeKey, storeValue, expirySeconds, RecordExistsAction.REPLACE);
    }

    public <T> void saveOrUpdate(final String storeName, final String storeKey, final T storeValue, final int expirySeconds) throws Exception {
        save(storeName, storeKey, storeValue, expirySeconds, RecordExistsAction.UPDATE);
    }

    public <T> void strictSave(final String storeName, final String storeKey, final T storeValue, final int expirySeconds) throws Exception {
        save(storeName, storeKey, storeValue, expirySeconds, RecordExistsAction.CREATE_ONLY);
    }

    public <T> Optional<T> get(final String storeName, final String storeKey, final Class<T> tClass) throws Exception {
        final Policy readPolicy = getReadPolicyPolicy();
        final Key key = getKey(storeName, storeKey);
        final Record record = aerospikeConnection.getClient().get(readPolicy, key, DEFAULT_BIN);

        if (null == record) {
            return Optional.empty();
        }
        return Optional.ofNullable(objectMapper.readValue(record.getString(DEFAULT_BIN), tClass));
    }

    public boolean delete(final String storeName, final String storeKey) {
        final WritePolicy writePolicy = getWritePolicyPolicy();
        final Key key = getKey(storeName, storeKey);
        return aerospikeConnection.getClient().delete(writePolicy, key);
    }

    private WritePolicy getWritePolicyPolicy() {
        return aerospikeConnection.getClient().getWritePolicyDefault();
    }

    private Policy getReadPolicyPolicy() {
        return aerospikeConnection.getClient().getReadPolicyDefault();
    }

    private Key getKey(final String storeName, final String storeKey) {
        final AerospikeConfiguration configuration = aerospikeConnection.getConfiguration();
        final String namespace = configuration.getNamespace();

        return new Key(namespace, storeName, storeKey);
    }

    private <T> Bin getBin(final T storeValue) throws JsonProcessingException {
        return new Bin(DEFAULT_BIN, objectMapper.writeValueAsString(storeValue));
    }
}
