package org.opendatadiscovery.discoverer.register;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendatadiscovery.client.ApiClient;
import org.opendatadiscovery.client.ApiException;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.MetadataExtension;
import org.opendatadiscovery.discoverer.MetadataDiscoverer;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.autoconfigure.ODDDiscovererProperties;
import org.opendatadiscovery.oddrn.model.NamedMicroservicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenDataDiscoveryRegister implements ApplicationListener<ContextRefreshedEvent> {
    private static final Log LOG = LogFactory.getLog(OpenDataDiscoveryRegister.class);

    private final List<MetadataDiscoverer> metadataDiscoverers;
    private final List<PathDiscoverer> pathDiscoverers;

    private final ApplicationContext context;
    private final ODDDiscovererProperties oddProperties;

    public OpenDataDiscoveryRegister(final List<MetadataDiscoverer> metadataDiscoverers,
                                     final List<PathDiscoverer> pathDiscoverers,
                                     final ApplicationContext applicationContext,
                                     final ODDDiscovererProperties properties) {
        this.metadataDiscoverers = metadataDiscoverers;
        this.pathDiscoverers = pathDiscoverers;
        this.context = applicationContext;
        this.oddProperties = properties;
    }

    @Override
    public void onApplicationEvent(final ContextRefreshedEvent event) {
        if (!validateProperties(oddProperties)) {
            return;
        }

        final Map<String, Object> metadata = metadataDiscoverers.stream()
            .flatMap(d -> d.metadata().entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));

//        final Map<String, Object> metadata = metadataDiscoverers.stream()
//            .flatMap(d -> d.metadata().entrySet().stream())
//            .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(
//                Map.Entry::getValue,
//                Collectors.reducing((v1, v2) -> v1))
//            ))
//            .entrySet()
//            .stream()
//            .filter(e -> e.getValue().isPresent())
//            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));

        final DataEntity dataEntity = new DataEntity()
            .metadata(Collections.singletonList(new MetadataExtension().metadata(metadata)))
            .oddrn(NamedMicroservicePath.builder()
                .name(String.format("%s/%s", oddProperties.getEnvironment(), context.getId()))
                .build()
                .oddrn()
            )
            .type(DataEntityType.MICROSERVICE)
            .name(context.getId())
            .dataTransformer(
                new DataTransformer()
                    .inputs(paths(PathDiscoverer.DiscoveryType.INPUT))
                    .outputs(paths(PathDiscoverer.DiscoveryType.OUTPUT))
            );

        final DataEntityList dataEntityList = new DataEntityList()
            .items(Collections.singletonList(dataEntity))
            .dataSourceOddrn(oddProperties.getDataSourceOddrn());

        final ApiClient apiClient = new ApiClient();
        apiClient.updateBaseUri(oddProperties.getOddPlatformHost());

        final OpenDataDiscoveryIngestionApi client = new OpenDataDiscoveryIngestionApi(apiClient);
        try {
            client.postDataEntityList(dataEntityList);
        } catch (final ApiException ignored) {
        }
    }

    private List<String> paths(PathDiscoverer.DiscoveryType type) {
        return pathDiscoverers.stream()
            .filter(p -> p.type().equals(type))
            .flatMap(p -> p.discover().stream())
            .map(OddrnPath::oddrn)
            .toList();
    }

    private boolean validateProperties(final ODDDiscovererProperties oddProperties) {
        if (!validateUrl(oddProperties.getOddPlatformHost())) {
            LOG.error("ODD Platform URL hasn't been defined");
            return false;
        }

        if (!StringUtils.hasLength(oddProperties.getDataSourceOddrn())) {
            LOG.error("Data source's ODDRN hasn't been defined");
            return false;
        }

        if (!StringUtils.hasLength(oddProperties.getEnvironment())) {
            LOG.error("Environment hasn't been defined");
            return false;
        }

        return true;
    }

    private boolean validateUrl(final String oddPlatformHost) {
        if (!StringUtils.hasLength(oddPlatformHost)) {
            return false;
        }

        try {
            URI.create(oddPlatformHost);
            return true;
        } catch (final Exception e) {
            return false;
        }
    }
}
