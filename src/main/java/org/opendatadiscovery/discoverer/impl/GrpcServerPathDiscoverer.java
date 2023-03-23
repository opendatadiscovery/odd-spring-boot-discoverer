package org.opendatadiscovery.discoverer.impl;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import net.devh.boot.grpc.server.service.GrpcServiceDefinition;
import net.devh.boot.grpc.server.service.GrpcServiceDiscoverer;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.impl.filter.GrpcServerServiceEndpointsFilter;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.oddrn.model.GrpcServicePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GrpcServerPathDiscoverer implements PathDiscoverer {
    private final GrpcServiceDiscoverer grpcServiceDiscoverer;
    private final GrpcServerServiceEndpointsFilter endpointsFilter;
    private final String serviceHost;

    public GrpcServerPathDiscoverer(final GrpcServiceDiscoverer grpcServiceDiscoverer,
                                    final GrpcServerServiceEndpointsFilter endpointsFilter,
                                    final String serviceHost) {
        this.grpcServiceDiscoverer = grpcServiceDiscoverer;
        this.endpointsFilter = endpointsFilter;
        this.serviceHost = serviceHost;
    }

    @Override
    public Paths discover() {
        final List<GrpcServicePath> grpcServicePaths = new ArrayList<>();

        for (final GrpcServiceDefinition grpcService : grpcServiceDiscoverer.findGrpcServices()) {
            if (!endpointsFilter.test(grpcService)) {
                continue;
            }

            final ServiceDescriptor serviceDescriptor = grpcService.getDefinition().getServiceDescriptor();
            for (final MethodDescriptor<?, ?> methodDescriptor : serviceDescriptor.getMethods()) {
                grpcServicePaths.add(GrpcServicePath.builder()
                    .host(serviceHost)
                    .service(serviceDescriptor.getName())
                    .method(methodDescriptor.getBareMethodName())
                    .build());
            }
        }

        return new Paths(grpcServicePaths, Collections.emptySet());
    }
}