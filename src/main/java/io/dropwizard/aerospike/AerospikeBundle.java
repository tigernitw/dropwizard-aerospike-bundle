package io.dropwizard.aerospike;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.aerospike.config.AerospikeConfiguration;
import io.dropwizard.aerospike.connectivity.AerospikeCommands;
import io.dropwizard.aerospike.connectivity.AerospikeConnection;
import io.dropwizard.aerospike.healthcheck.AerospikeHealthCheck;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.val;

public abstract class AerospikeBundle <T extends Configuration> implements ConfiguredBundle<T> {

    private AerospikeConnection connection;
    @Getter
    private AerospikeCommands aerospikeCommands;

    @Override
    public void run(T t, Environment environment) throws Exception {
        val configuration = getAerospikeConfig(t);
        val mapper = getMapperConfig(t);
        connection = new AerospikeConnection(configuration);
        environment.lifecycle().manage(connection);
        AerospikeHealthCheck healthCheck = new AerospikeHealthCheck(connection);
        environment.healthChecks().register("aerospike-healthCheck", healthCheck);
        aerospikeCommands = new AerospikeCommands(connection, mapper);
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
    }

    protected abstract AerospikeConfiguration getAerospikeConfig(T t);

    protected abstract ObjectMapper getMapperConfig(T t);

}
