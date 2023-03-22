package org.opendatadiscovery.discoverer.impl;

import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

public class KafkaStreamsDiscoverer implements PathDiscoverer {
    private static final Log LOG = LogFactory.getLog(KafkaStreamsDiscoverer.class);

    private final List<StreamsBuilderFactoryBean> builderFactoryBeans;

    public KafkaStreamsDiscoverer(final List<StreamsBuilderFactoryBean> builderFactoryBeans) {
        this.builderFactoryBeans = builderFactoryBeans;
    }

    @Override
    public Paths discover() {
        final List<Paths> paths = new ArrayList<>();

        for (final StreamsBuilderFactoryBean factoryBean : builderFactoryBeans) {
            final Topology topology = factoryBean.getTopology();
            if (topology == null) {
                LOG.warn(String.format("Topology is null in %s", factoryBean.getClass().getName()));
                continue;
            }

            if (factoryBean.getStreamsConfiguration() == null) {
                LOG.warn(String.format("Streams configuration is null in %s", factoryBean.getClass().getName()));
                continue;
            }

            final TopologyDescription topologyDescription = topology.describe();

            final Object clusterConfig = factoryBean.getStreamsConfiguration()
                .get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

            final List<String> bootstrapServers = clusterConfig instanceof String ?
                Arrays.asList(((String) clusterConfig).split(",")) :
                    clusterConfig instanceof List<?> ? (List<String>) clusterConfig : List.of();

            final String cluster = bootstrapServers.stream()
                .map(s -> s.replaceFirst("PLAINTEXT://", "").replaceFirst("SSL://", ""))
                .sorted()
                .collect(Collectors.joining(","));

            final Set<KafkaPath> globalStoreTopics = topologyDescription.globalStores().stream()
                .flatMap(gs -> gs.source().topicSet().stream())
                .distinct()
                .map(topic -> buildKafkaPath(cluster, topic))
                .collect(Collectors.toSet());

            paths.add(new Paths(globalStoreTopics, Collections.emptySet()));

            final Map<TopologyNodeType, Set<KafkaPath>> map = topologyDescription.subtopologies().stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .flatMap(node -> extractNodes(node, cluster))
                .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, Collectors.toSet())));

            paths.add(new Paths(
                map.getOrDefault(TopologyNodeType.INPUT, Collections.emptySet()),
                map.getOrDefault(TopologyNodeType.OUTPUT, Collections.emptySet())
            ));
        }

        return Paths.merge(paths);
    }

    private Stream<Map.Entry<TopologyNodeType, KafkaPath>> extractNodes(final TopologyDescription.Node node,
                                                                        final String cluster) {
        if (node instanceof TopologyDescription.Sink) {
            return Stream.of(Map.entry(
                TopologyNodeType.OUTPUT,
                buildKafkaPath(cluster, ((TopologyDescription.Sink) node).topic())
            ));
        }

        if (node instanceof TopologyDescription.Source) {
            final TopologyDescription.Source source = (TopologyDescription.Source) node;

            return source.topicSet()
                .stream()
                .map(topic -> buildKafkaPath(cluster, topic))
                .map(oddrn -> Map.entry(TopologyNodeType.INPUT, oddrn));
        }

        return Stream.empty();
    }

    private KafkaPath buildKafkaPath(final String cluster, final String topic) {
        return KafkaPath.builder().cluster(cluster).topic(topic).build();
    }

    private enum TopologyNodeType {
        INPUT, OUTPUT
    }
}