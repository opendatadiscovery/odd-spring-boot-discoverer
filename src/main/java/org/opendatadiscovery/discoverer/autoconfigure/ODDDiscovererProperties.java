package org.opendatadiscovery.discoverer.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opendatadiscovery.discoverer")
public class ODDDiscovererProperties {
    private boolean enabled;
    private String oddPlatformHost;
    private String dataSourceOddrn;
    private String environment;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    public String getOddPlatformHost() {
        return oddPlatformHost;
    }

    public void setOddPlatformHost(final String oddPlatformHost) {
        this.oddPlatformHost = oddPlatformHost;
    }

    public String getDataSourceOddrn() {
        return dataSourceOddrn;
    }

    public void setDataSourceOddrn(final String dataSourceOddrn) {
        this.dataSourceOddrn = dataSourceOddrn;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(final String environment) {
        this.environment = environment;
    }
}
