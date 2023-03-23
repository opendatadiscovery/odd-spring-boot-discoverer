package org.opendatadiscovery.discoverer.impl.filter;

import net.devh.boot.grpc.server.service.GrpcServiceDefinition;

import java.util.Set;
import java.util.function.Predicate;

public class GrpcServerServiceEndpointsFilter implements Predicate<GrpcServiceDefinition> {
    private static final Set<String> BLACKLIST = Set.of(
        "grpc.health.v1.Health",
        "grpc.reflection.v1alpha.ServerReflection",
        "grpc.channelz.v1.Channelz"
    );

    @Override
    public boolean test(final GrpcServiceDefinition grpcServiceDefinition) {
        return !BLACKLIST.contains(grpcServiceDefinition.getDefinition().getServiceDescriptor().getName());
    }
}
