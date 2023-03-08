package org.opendatadiscovery.discoverer.impl;

import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaListenerDiscoverer implements PathDiscoverer {
    private final KafkaProperties kafkaProperties;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    public KafkaListenerDiscoverer(final KafkaProperties kafkaProperties,
                                   final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        this.kafkaProperties = kafkaProperties;
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    @Override
    public List<? extends OddrnPath> discover() {
        final String cluster = kafkaProperties.getBootstrapServers().stream()
            .map(s -> s.replaceFirst("PLAINTEXT://", "").replaceFirst("SSL://", ""))
            .sorted()
            .collect(Collectors.joining(","));

        final KafkaPath kafkaPath = KafkaPath.builder().cluster(cluster).build();

        return kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
            .flatMap(c -> Arrays.stream(Objects.requireNonNull(c.getContainerProperties().getTopics())))
            .distinct()
            .map(t -> kafkaPath.toBuilder().topic(t).build())
            .collect(Collectors.toList());
    }

    @Override
    public DiscoveryType type() {
        return DiscoveryType.INPUT;
    }
}