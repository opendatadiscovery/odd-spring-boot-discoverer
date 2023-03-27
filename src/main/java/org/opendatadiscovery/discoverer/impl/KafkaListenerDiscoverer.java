package org.opendatadiscovery.discoverer.impl;

import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
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
    public Paths discover() {
        final String cluster = kafkaProperties.getBootstrapServers().stream()
            .map(server -> URI.create(server).getHost())
            .distinct()
            .sorted()
            .collect(Collectors.joining(","));

        final KafkaPath kafkaPath = KafkaPath.builder().cluster(cluster).build();

        final List<KafkaPath> inputs = kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
            .flatMap(c -> Arrays.stream(Objects.requireNonNull(c.getContainerProperties().getTopics())))
            .distinct()
            .map(t -> kafkaPath.toBuilder().topic(t).build())
            .collect(Collectors.toList());

        return new Paths(inputs, Collections.emptyList());
    }
}