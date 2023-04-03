package com.azure.spring.problem.reproduce.service.bus;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.spring.cloud.service.servicebus.consumer.ServiceBusErrorHandler;
import com.azure.spring.integration.servicebus.inbound.ServiceBusInboundChannelAdapter;
import com.azure.spring.messaging.AzureHeaders;
import com.azure.spring.messaging.checkpoint.Checkpointer;
import com.azure.spring.messaging.servicebus.core.DefaultServiceBusNamespaceProcessorFactory;
import com.azure.spring.messaging.servicebus.core.DefaultServiceBusNamespaceProducerFactory;
import com.azure.spring.messaging.servicebus.core.ServiceBusProcessorFactory;
import com.azure.spring.messaging.servicebus.core.ServiceBusProducerFactory;
import com.azure.spring.messaging.servicebus.core.ServiceBusTemplate;
import com.azure.spring.messaging.servicebus.core.listener.ServiceBusMessageListenerContainer;
import com.azure.spring.messaging.servicebus.core.properties.NamespaceProperties;
import com.azure.spring.messaging.servicebus.core.properties.ServiceBusContainerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;

@Configuration
@EnableConfigurationProperties(ServiceBusConfigProperties.class)
public class ServiceBusQueueConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBusQueueConfiguration.class);
    private static final String INPUT_CHANNEL = "inputchannel";
    private final NamespaceProperties firstNamespaceProperties;

    public ServiceBusQueueConfiguration(ServiceBusConfigProperties configProperties) {
        this.firstNamespaceProperties = configProperties.getProperties();
    }

    @Bean(name = INPUT_CHANNEL)
    public MessageChannel input() {
        return new DirectChannel();
    }

    @Bean(name = "serviceBusErrorHandler")
    public ServiceBusErrorHandler serviceBusErrorHandler() {
        return t -> LOGGER.error("Something went wrong with the ServiceBusListener : " + t);
    }

    @Bean
    public ServiceBusTemplate firstServiceBusTemplate() {
        ServiceBusProducerFactory producerFactory = new DefaultServiceBusNamespaceProducerFactory(firstNamespaceProperties);
        return new ServiceBusTemplate(producerFactory);
    }

    @Bean("serviceBusListenerContainerProperties")
    ServiceBusContainerProperties containerProperties(
            @Qualifier("serviceBusErrorHandler") ServiceBusErrorHandler serviceBusErrorHandler) {
        ServiceBusContainerProperties containerProperties = new ServiceBusContainerProperties();
        containerProperties.setConnectionString(firstNamespaceProperties.getConnectionString());
        containerProperties.setEntityName(firstNamespaceProperties.getEntityName());
        containerProperties.setAutoComplete(false);
        containerProperties.setPrefetchCount(10);
        containerProperties.setErrorHandler(serviceBusErrorHandler);
        containerProperties.getClient().setTransportType(AmqpTransportType.AMQP_WEB_SOCKETS);
        return containerProperties;
    }

    @Bean("queueListenerContainer")
    public ServiceBusMessageListenerContainer messageListenerContainer(
            @Qualifier("serviceBusListenerContainerProperties") ServiceBusContainerProperties containerProperties) {
        ServiceBusProcessorFactory processorFactory = new DefaultServiceBusNamespaceProcessorFactory(firstNamespaceProperties);
        return new ServiceBusMessageListenerContainer(processorFactory, containerProperties);
    }

    @Bean("queueMessageChannelAdapter")
    public ServiceBusInboundChannelAdapter queueMessageChannelAdapter(
            @Qualifier(INPUT_CHANNEL) MessageChannel inputChannel,
            @Qualifier("queueListenerContainer") ServiceBusMessageListenerContainer listenerContainer) {
        ServiceBusInboundChannelAdapter adapter = new ServiceBusInboundChannelAdapter(listenerContainer);
        adapter.setOutputChannel(inputChannel);
        adapter.setPayloadType(String.class);
        return adapter;
    }

    @ServiceActivator(inputChannel = INPUT_CHANNEL)
    public void receiveMessage(String message, @Header(AzureHeaders.CHECKPOINTER) Checkpointer checkpointer) {
        LOGGER.info("New message received: '{}'.", message);
        checkpointer.success()
                .doOnSuccess(s -> LOGGER.info("Message '{}' successfully checkpointed.", message))
                .doOnError(e -> LOGGER.error("Error found.", e))
                .block();
    }
}
