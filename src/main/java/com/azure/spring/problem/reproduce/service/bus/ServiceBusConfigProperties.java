package com.azure.spring.problem.reproduce.service.bus;

import com.azure.spring.messaging.servicebus.core.properties.NamespaceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("problem.reproduce.service.bus")
public class ServiceBusConfigProperties {

    private NamespaceProperties properties;

    public NamespaceProperties getProperties() {
        return properties;
    }

    public void setProperties(NamespaceProperties properties) {
        this.properties = properties;
    }

}
