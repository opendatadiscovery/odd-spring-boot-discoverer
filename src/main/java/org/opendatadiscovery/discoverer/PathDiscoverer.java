package org.opendatadiscovery.discoverer;

import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.List;

public interface PathDiscoverer {
    List<? extends OddrnPath> discover();

    DiscoveryType type();

    enum DiscoveryType {
        INPUT, OUTPUT
    }
}
