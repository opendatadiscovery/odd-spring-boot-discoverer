package org.opendatadiscovery.discoverer.autoconfigure;

import net.devh.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration;
import net.devh.boot.grpc.client.config.GrpcChannelsProperties;
import net.devh.boot.grpc.server.service.GrpcServiceDiscoverer;
import org.apache.kafka.streams.StreamsBuilder;
import org.opendatadiscovery.discoverer.AdditionalEntitiesDiscoverer;
import org.opendatadiscovery.discoverer.MetadataDiscoverer;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.impl.BuildInfoDiscoverer;
import org.opendatadiscovery.discoverer.impl.GitInfoDiscoverer;
import org.opendatadiscovery.discoverer.impl.GrpcClientPathDiscoverer;
import org.opendatadiscovery.discoverer.impl.GrpcServerAdditionalEntitiesDiscoverer;
import org.opendatadiscovery.discoverer.impl.GrpcServerPathDiscoverer;
import org.opendatadiscovery.discoverer.impl.KafkaListenerDiscoverer;
import org.opendatadiscovery.discoverer.impl.KafkaStreamsDiscoverer;
import org.opendatadiscovery.discoverer.impl.filter.GrpcServerServiceEndpointsFilter;
import org.opendatadiscovery.discoverer.model.grpc.GrpcClientDescriptorRegistry;
import org.opendatadiscovery.discoverer.registrar.OpenDataDiscoveryRegistrar;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.info.ProjectInfoAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.List;

@Configuration
@EnableConfigurationProperties(ODDDiscovererProperties.class)
@ConditionalOnProperty(value = "opendatadiscovery.enabled", havingValue = "true")
@AutoConfigureAfter({
    ProjectInfoAutoConfiguration.class,
    KafkaAutoConfiguration.class
})
public class ODDDiscovererAutoConfiguration {
    @Bean
    public OpenDataDiscoveryRegistrar register(
        final List<MetadataDiscoverer> metadataDiscoverers,
        final List<PathDiscoverer> pathDiscoverers,
        final List<AdditionalEntitiesDiscoverer> additionalEntitiesDiscoverers,
        final ApplicationContext context,
        final ODDDiscovererProperties properties
    ) {
        return new OpenDataDiscoveryRegistrar(
            metadataDiscoverers,
            pathDiscoverers,
            additionalEntitiesDiscoverers,
            context,
            properties
        );
    }

    @Bean
    @ConditionalOnBean(BuildProperties.class)
    public MetadataDiscoverer buildInfo(final BuildProperties buildProperties) {
        return new BuildInfoDiscoverer(buildProperties);
    }

    @Bean
    @ConditionalOnBean(GitProperties.class)
    public MetadataDiscoverer gitInfo(final GitProperties gitProperties) {
        return new GitInfoDiscoverer(gitProperties);
    }

    @ConditionalOnBean(GrpcServiceDiscoverer.class)
    @ConditionalOnProperty(value = "opendatadiscovery.bind.hostname")
    static class GrpcServerDiscovererConfiguration {
        private final GrpcServiceDiscoverer grpcServiceDiscoverer;
        private final String bindHostname;
        private final GrpcServerServiceEndpointsFilter endpointsFilter;

        GrpcServerDiscovererConfiguration(final GrpcServiceDiscoverer grpcServiceDiscoverer,
                                          final ODDDiscovererProperties oddDiscovererProperties) {
            this.grpcServiceDiscoverer = grpcServiceDiscoverer;
            this.bindHostname = oddDiscovererProperties.getBind().getHostname();
            this.endpointsFilter = new GrpcServerServiceEndpointsFilter();
        }

        @Bean
        public PathDiscoverer grpcServerPathDiscoverer() {
            return new GrpcServerPathDiscoverer(grpcServiceDiscoverer, endpointsFilter, bindHostname);
        }

        @Bean
        public AdditionalEntitiesDiscoverer grpcServerAdditionalEntitiesDiscoverer() {
            return new GrpcServerAdditionalEntitiesDiscoverer(grpcServiceDiscoverer, endpointsFilter, bindHostname);
        }
    }

    @ConditionalOnBean(GrpcClientDescriptorRegistry.class)
    @ConditionalOnClass(GrpcClientAutoConfiguration.class)
    static class GrpcDiscovererConfiguration {
        @Bean
        public PathDiscoverer grpcClientPathDiscoverer(final GrpcChannelsProperties channelsProperties,
                                                       final GrpcClientDescriptorRegistry clientDescriptorRegistry) {
            return new GrpcClientPathDiscoverer(channelsProperties, clientDescriptorRegistry);
        }
    }

    @ConditionalOnBean(KafkaListenerEndpointRegistry.class)
    @ConditionalOnClass(KafkaListenerEndpointRegistry.class)
    static class KafkaDiscovererConfiguration {
        @Bean
        public PathDiscoverer kafka(final KafkaProperties kafkaProperties,
                                    final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
            return new KafkaListenerDiscoverer(kafkaProperties, kafkaListenerEndpointRegistry);
        }
    }

    @ConditionalOnBean(StreamsBuilderFactoryBean.class)
    @ConditionalOnClass(StreamsBuilder.class)
    static class KafkaStreamsDiscovererConfiguration {
        @Bean
        public PathDiscoverer kafkaStreams(final List<StreamsBuilderFactoryBean> builderFactoryBeans) {
            return new KafkaStreamsDiscoverer(builderFactoryBeans);
        }
    }
}
