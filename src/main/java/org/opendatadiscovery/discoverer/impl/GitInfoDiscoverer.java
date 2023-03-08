package org.opendatadiscovery.discoverer.impl;

import org.springframework.boot.info.GitProperties;
import org.springframework.boot.info.InfoProperties;

public class GitInfoDiscoverer extends AbstractInfoDiscoverer {
    private final GitProperties gitProperties;

    public GitInfoDiscoverer(final GitProperties gitProperties) {
        this.gitProperties = gitProperties;
    }

    @Override
    protected InfoProperties getInfoProperties() {
        return gitProperties;
    }
}
