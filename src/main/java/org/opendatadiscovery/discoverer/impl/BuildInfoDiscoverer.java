package org.opendatadiscovery.discoverer.impl;

import org.springframework.boot.info.BuildProperties;
import org.springframework.boot.info.InfoProperties;

public class BuildInfoDiscoverer extends AbstractInfoDiscoverer {
    private final BuildProperties buildProperties;

    public BuildInfoDiscoverer(final BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    @Override
    protected InfoProperties getInfoProperties() {
        return buildProperties;
    }
}