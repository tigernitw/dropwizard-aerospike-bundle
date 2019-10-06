package io.dropwizard.aerospike.config;

import lombok.Data;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.List;

@Data
public class AerospikeConfiguration {

    @NotNull
    @Size(min = 1)
    private List<HostConfig> hostConfigs;

    private List<String> tlsProtocols;

    @NotNull
    @NotEmpty
    private String namespace;

    private String username;

    private String password;

    @Min(1)
    @Max(10)
    private int maxConnectionsPerNode = 5;

    @Min(0)
    @Max(1024)
    private int threadPoolSize;

    @Min(1)
    @Max(10000)
    private int maxSocketIdle = 1000;

    @Min(1)
    @Max(1000)
    private int timeout = 1000;

    @Min(0)
    @Max(10)
    private int retries = 5;

    @Min(10)
    @Max(1000)
    private int sleepBetweenRetries = 100;

}
