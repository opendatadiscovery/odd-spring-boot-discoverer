package org.opendatadiscovery.discoverer.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opendatadiscovery")
public class ODDDiscovererProperties {
    private boolean enabled;
    private String url;
    private String dataSourceOddrn;
    private String environment;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(final String url) {
        this.url = url;
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
