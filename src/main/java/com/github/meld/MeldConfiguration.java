package com.github.meld;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.Configuration;

public class MeldConfiguration extends Configuration {
    @Valid
    @NotNull
    @JsonProperty
    private String swaggerBasePath;
    public String getSwaggerBasePath(){ return swaggerBasePath; }
}
