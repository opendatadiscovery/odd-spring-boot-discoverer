package org.opendatadiscovery.discoverer.register;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendatadiscovery.client.ApiClient;
import org.opendatadiscovery.client.ApiException;
import org.opendatadiscovery.client.ApiResponse;
import org.opendatadiscovery.client.api.OpenDataDiscoveryIngestionApi;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityList;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataTransformer;
import org.opendatadiscovery.client.model.MetadataExtension;
import org.opendatadiscovery.discoverer.MetadataDiscoverer;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.autoconfigure.ODDDiscovererProperties;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.oddrn.model.NamedMicroservicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OpenDataDiscoveryRegister implements ApplicationListener<ApplicationReadyEvent> {
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
    public void onApplicationEvent(final ApplicationReadyEvent event) {
        if (!validateProperties(oddProperties)) {
            return;
        }

        final Paths paths = extractPaths();

        final DataEntity dataEntity = new DataEntity()
            .metadata(Collections.singletonList(new MetadataExtension().metadata(extractMetadata())))
            .oddrn(NamedMicroservicePath.builder()
                .name(String.format("%s/%s", oddProperties.getEnvironment(), context.getId()))
                .build()
                .oddrn()
            )
            .type(DataEntityType.MICROSERVICE)
            .name(context.getId())
            .dataTransformer(
                new DataTransformer()
                    .inputs(paths.getInputs().stream().map(OddrnPath::oddrn).collect(Collectors.toList()))
                    .outputs(paths.getOutputs().stream().map(OddrnPath::oddrn).collect(Collectors.toList()))
            );

        final DataEntityList dataEntityList = new DataEntityList()
            .items(Collections.singletonList(dataEntity))
            .dataSourceOddrn(oddProperties.getDataSourceOddrn());

        LOG.debug("Payload to send: " + dataEntityList);

        final ApiClient apiClient = new ApiClient();
        apiClient.updateBaseUri(oddProperties.getUrl());

        final OpenDataDiscoveryIngestionApi client = new OpenDataDiscoveryIngestionApi(apiClient);
        try {
            final ApiResponse<Void> response = client.postDataEntityListWithHttpInfo(dataEntityList);
            if (response.getStatusCode() != 200) {
                LOG.warn("ODD Platform responded with code: " + response.getStatusCode());
                return;
            }
            LOG.info("Payload has been successfully sent to the ODD Platform: " + oddProperties.getUrl());
        } catch (final ApiException e) {
            LOG.error("Couldn't send payload to the ODD Platform", e);
        }
    }

    private Paths extractPaths() {
        if (pathDiscoverers.isEmpty()) {
            return Paths.empty();
        }

        final List<Paths> total = new ArrayList<>();
        for (final PathDiscoverer pathDiscoverer : pathDiscoverers) {
            try {
                final Paths paths = pathDiscoverer.discover();
                if (paths != null) {
                    total.add(paths);
                }

                total.add(pathDiscoverer.discover());
            } catch (final Throwable t) {
                LOG.error(String.format("Couldn't extract paths using %s", pathDiscoverer.getClass().getName()), t);
            }
        }

        return Paths.merge(total);
    }

    private Map<String, Object> extractMetadata() {
        if (metadataDiscoverers.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Object> total = new HashMap<>();
        for (final MetadataDiscoverer discoverer : metadataDiscoverers) {
            try {
                final Map<String, Object> metadata = discoverer.metadata();
                if (!CollectionUtils.isEmpty(metadata)) {
                    total.putAll(metadata);
                }
            } catch (final Throwable t) {
                LOG.error(String.format("Couldn't extract metadata using %s", discoverer.getClass().getName()), t);
            }
        }

        return total;
    }

    private boolean validateProperties(final ODDDiscovererProperties oddProperties) {
        if (!validateUrl(oddProperties.getUrl())) {
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
