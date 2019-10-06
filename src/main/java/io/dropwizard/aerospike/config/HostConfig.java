package io.dropwizard.aerospike.config;

import lombok.Data;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
public class HostConfig {

    @NotNull
    @NotEmpty
    private String host;

    @Min(1)
    private int port;

    private String tlsName;
}
