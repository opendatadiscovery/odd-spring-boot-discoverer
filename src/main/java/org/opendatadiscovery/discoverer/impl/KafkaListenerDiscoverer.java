package org.opendatadiscovery.discoverer.impl;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;

public class KafkaListenerDiscoverer implements PathDiscoverer {
    private static final Log LOG = LogFactory.getLog(KafkaListenerDiscoverer.class);

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
            .map(this::extractHostname)
            .distinct()
            .sorted()
            .collect(Collectors.joining(","));

        final KafkaPath kafkaPath = KafkaPath.builder().cluster(cluster).build();

        final List<KafkaPath> inputs = kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
            .map(this::extractContainerProperties)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(this::extractTopics)
            .flatMap(Collection::stream)
            .distinct()
            .map(t -> kafkaPath.toBuilder().topic(t).build())
            .collect(Collectors.toList());

        return new Paths(inputs, Collections.emptyList());
    }

    private String extractHostname(final String bootstrapServer) {
        return bootstrapServer.contains("://")
            ? URI.create(bootstrapServer).getHost()
            : bootstrapServer.split(":")[0];
    }

    private Collection<String> extractTopics(final ContainerProperties containerProperties) {
        final HashSet<String> resultTopics = new HashSet<>();

        final String[] topics = containerProperties.getTopics();
        if (topics != null) {
            Collections.addAll(resultTopics, topics);
        }

        final TopicPartitionOffset[] topicPartitions = containerProperties.getTopicPartitions();
        if (topicPartitions != null) {
            for (final TopicPartitionOffset topicPartition : topicPartitions) {
                resultTopics.add(topicPartition.getTopic());
            }
        }

        return resultTopics;
    }

    private Optional<ContainerProperties> extractContainerProperties(final MessageListenerContainer container) {
        try {
            return Optional.of(container.getContainerProperties());
        } catch (final Exception e) {
            LOG.error("Couldn't extract container properties from MessageListenerContainer", e);
            return Optional.empty();
        }
    }
}