package org.opendatadiscovery.discoverer.model.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class GrpcClientDescriptor {
    private final String clientName;
    private final ServiceDescriptor serviceDescriptor;
    private final Map<String, MethodDescriptor<?, ?>> whitelistedMethodDescriptors;

    public GrpcClientDescriptor(final String clientName, final ServiceDescriptor serviceDescriptor) {
        this(clientName, serviceDescriptor, null);
    }

    public GrpcClientDescriptor(final String clientName,
                                final ServiceDescriptor serviceDescriptor,
                                final Collection<MethodDescriptor<?, ?>> whitelistedMethodDescriptors) {
        this.serviceDescriptor = requireNonNull(serviceDescriptor, "Service descriptor cannot be null");
        this.clientName = requireNonNull(clientName, "Client name cannot be null");
        this.whitelistedMethodDescriptors = CollectionUtils.emptyIfNull(whitelistedMethodDescriptors)
            .stream()
            .collect(Collectors.toMap(MethodDescriptor::getBareMethodName, identity()));
    }

    public boolean isMethodWhitelisted(final String methodBareName) {
        return this.whitelistedMethodDescriptors.containsKey(methodBareName);
    }

    public ServiceDescriptor getServiceDescriptor() {
        return serviceDescriptor;
    }

    public Map<String, MethodDescriptor<?, ?>> getWhitelistedMethodDescriptors() {
        return whitelistedMethodDescriptors;
    }

    public String getClientName() {
        return clientName;
    }
}
