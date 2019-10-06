package io.dropwizard.aerospike.connectivity;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.*;
import io.dropwizard.aerospike.config.AerospikeConfiguration;
import io.dropwizard.aerospike.utils.CommonUtils;
import io.dropwizard.lifecycle.Managed;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Executors;

@Slf4j
@Getter
public class AerospikeConnection implements Managed {

    private final AerospikeConfiguration configuration;
    private IAerospikeClient client;

    public AerospikeConnection(final AerospikeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void start() throws Exception {
        log.info("Starting Connecting to Aerospike client with config : {}", configuration.getHostConfigs());

        final Policy readPolicy = new Policy();
        readPolicy.maxRetries = configuration.getRetries();
        readPolicy.readModeAP = ReadModeAP.ONE;
        readPolicy.replica = Replica.MASTER_PROLES;
        readPolicy.sleepBetweenRetries = configuration.getSleepBetweenRetries();
        readPolicy.totalTimeout = configuration.getTimeout();
        readPolicy.sendKey = true;

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.maxRetries = configuration.getRetries();
        writePolicy.readModeAP = ReadModeAP.ALL;
        writePolicy.replica = Replica.MASTER_PROLES;
        writePolicy.sleepBetweenRetries = configuration.getSleepBetweenRetries();
        writePolicy.commitLevel = CommitLevel.COMMIT_ALL;
        writePolicy.totalTimeout = configuration.getTimeout();
        writePolicy.sendKey = true;

        final ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = configuration.getUsername();
        clientPolicy.password = configuration.getPassword();
        clientPolicy.readPolicyDefault = readPolicy;
        clientPolicy.writePolicyDefault = writePolicy;
        clientPolicy.threadPool = Executors.newFixedThreadPool(configuration.getThreadPoolSize());
        clientPolicy.maxConnsPerNode = configuration.getMaxConnectionsPerNode();
        clientPolicy.maxSocketIdle = configuration.getMaxSocketIdle();
        clientPolicy.failIfNotConnected = true;

        final List<String> tlsProtocols = configuration.getTlsProtocols();
        if (!CommonUtils.isEmpty(tlsProtocols)) {
            clientPolicy.tlsPolicy = new TlsPolicy(); //Default protocols new String[] {"TLSv1.2"};
            clientPolicy.tlsPolicy.protocols = tlsProtocols.toArray(new String[tlsProtocols.size()]);
        }

        client = new AerospikeClient(
                clientPolicy, configuration.getHostConfigs().stream().map
                (
                        hostConfig -> new Host(hostConfig.getHost(),
                                hostConfig.getTlsName(),
                                hostConfig.getPort()
                        )
                )
                .toArray(Host[]::new)
        );
        log.info("Aerospike connectivity status: " + client.isConnected());
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping Aerospike client");
        if (client != null) {
            client.close();
        }
        log.info("Stopped Aerospike client");
    }
}
