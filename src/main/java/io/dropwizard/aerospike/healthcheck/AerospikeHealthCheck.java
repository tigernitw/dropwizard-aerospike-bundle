package io.dropwizard.aerospike.healthcheck;


import com.codahale.metrics.health.HealthCheck;
import io.dropwizard.aerospike.connectivity.AerospikeConnection;

public class AerospikeHealthCheck extends HealthCheck {

    private AerospikeConnection connection;

    public AerospikeHealthCheck(final AerospikeConnection connection) {
        this.connection = connection;
    }

    @Override
    protected Result check() throws Exception {
        return connection.getClient().isConnected() ?
                HealthCheck.Result.healthy() : HealthCheck.Result.unhealthy("Aerospike connectivity error");
    }
}
