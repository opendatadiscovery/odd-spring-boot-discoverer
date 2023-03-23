package org.opendatadiscovery.discoverer.impl;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import net.devh.boot.grpc.server.service.GrpcServiceDefinition;
import net.devh.boot.grpc.server.service.GrpcServiceDiscoverer;
import org.opendatadiscovery.client.model.DataEntity;
import org.opendatadiscovery.client.model.DataEntityGroup;
import org.opendatadiscovery.client.model.DataEntityType;
import org.opendatadiscovery.client.model.DataInput;
import org.opendatadiscovery.discoverer.AdditionalEntitiesDiscoverer;
import org.opendatadiscovery.discoverer.impl.filter.GrpcServerServiceEndpointsFilter;
import org.opendatadiscovery.oddrn.model.GrpcServicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GrpcServerAdditionalEntitiesDiscoverer implements AdditionalEntitiesDiscoverer {
    private final GrpcServiceDiscoverer grpcServiceDiscoverer;
    private final GrpcServerServiceEndpointsFilter endpointsFilter;
    private final String serviceHost;

    public GrpcServerAdditionalEntitiesDiscoverer(final GrpcServiceDiscoverer grpcServiceDiscoverer,
                                                  final GrpcServerServiceEndpointsFilter endpointsFilter,
                                                  final String serviceHost) {
        this.grpcServiceDiscoverer = grpcServiceDiscoverer;
        this.endpointsFilter = endpointsFilter;
        this.serviceHost = serviceHost;
    }

    @Override
    public List<DataEntity> discover() {
        final List<DataEntity> dataEntities = new ArrayList<>();

        for (final GrpcServiceDefinition grpcService : grpcServiceDiscoverer.findGrpcServices()) {
            if (!endpointsFilter.test(grpcService)) {
                continue;
            }

            final ServiceDescriptor serviceDescriptor = grpcService.getDefinition().getServiceDescriptor();
            final List<DataEntity> serviceDataInputs = new ArrayList<>();
            final GrpcServicePath servicePath = GrpcServicePath.builder()
                .host(serviceHost)
                .service(serviceDescriptor.getName())
                .build();

            for (final MethodDescriptor<?, ?> methodDescriptor : serviceDescriptor.getMethods()) {
                final GrpcServicePath dataInputOddrn = servicePath.toBuilder()
                    .method(methodDescriptor.getBareMethodName())
                    .build();

                serviceDataInputs.add(createDataInput(dataInputOddrn, methodDescriptor.getBareMethodName()));
            }

            final List<String> serviceDataInputOddrns = serviceDataInputs
                .stream()
                .distinct()
                .map(DataEntity::getOddrn)
                .collect(Collectors.toList());

            dataEntities.addAll(serviceDataInputs);
            dataEntities.add(createApiService(servicePath, serviceDescriptor.getName(), serviceDataInputOddrns));
        }

        return dataEntities;
    }

    private DataEntity createDataInput(final OddrnPath oddrn, final String name) {
        return new DataEntity()
            .oddrn(oddrn.oddrn())
            .name(name)
            .type(DataEntityType.API_CALL)
            .dataInput(new DataInput().outputs(List.of()));
    }

    private DataEntity createApiService(final OddrnPath oddrn, final String name, final List<String> entities) {
        return new DataEntity()
            .oddrn(oddrn.oddrn())
            .name(name)
            .type(DataEntityType.API_SERVICE)
            .dataEntityGroup(new DataEntityGroup().entitiesList(entities));
    }
}