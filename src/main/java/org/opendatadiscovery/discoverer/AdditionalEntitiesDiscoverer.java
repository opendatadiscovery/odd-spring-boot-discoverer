package org.opendatadiscovery.discoverer;

import org.opendatadiscovery.client.model.DataEntity;

import java.util.List;

public interface AdditionalEntitiesDiscoverer {
    List<DataEntity> discover();
}
