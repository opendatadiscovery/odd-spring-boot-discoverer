package org.opendatadiscovery.discoverer.impl;

import org.opendatadiscovery.discoverer.MetadataDiscoverer;
import org.springframework.boot.info.InfoProperties;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class AbstractInfoDiscoverer implements MetadataDiscoverer {
    @Override
    public Map<String, Object> metadata() {
        return StreamSupport
            .stream(getInfoProperties().spliterator(), false)
            .collect(Collectors.toMap(InfoProperties.Entry::getKey, InfoProperties.Entry::getValue));
    }

    protected abstract InfoProperties getInfoProperties();
}
