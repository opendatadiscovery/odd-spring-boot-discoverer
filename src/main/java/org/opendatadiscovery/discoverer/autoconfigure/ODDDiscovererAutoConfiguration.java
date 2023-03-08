package org.opendatadiscovery.discoverer.autoconfigure;

import org.opendatadiscovery.discoverer.MetadataDiscoverer;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.impl.BuildInfoDiscoverer;
import org.opendatadiscovery.discoverer.impl.GitInfoDiscoverer;
import org.opendatadiscovery.discoverer.impl.KafkaListenerDiscoverer;
import org.opendatadiscovery.discoverer.register.OpenDataDiscoveryRegister;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.GitProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.List;

@Configuration
@EnableConfigurationProperties(ODDDiscovererProperties.class)
@ConditionalOnProperty(value = "opendatadiscovery.enabled", havingValue = "true")
public class ODDDiscovererAutoConfiguration {
    @Bean
    public OpenDataDiscoveryRegister register(
        final List<MetadataDiscoverer> metadataDiscoverers,
        final List<PathDiscoverer> pathDiscoverers,
        final ApplicationContext context,
        final ODDDiscovererProperties properties
    ) {
        return new OpenDataDiscoveryRegister(metadataDiscoverers, pathDiscoverers, context, properties);
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

    @ConditionalOnBean(KafkaListenerEndpointRegistry.class)
    @ConditionalOnClass(KafkaListenerEndpointRegistry.class)
    static class KafkaDiscovererConfiguration {
        @Bean
        public PathDiscoverer kafka(final KafkaProperties kafkaProperties,
                                    final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
            return new KafkaListenerDiscoverer(kafkaProperties, kafkaListenerEndpointRegistry);
        }
    }
}
