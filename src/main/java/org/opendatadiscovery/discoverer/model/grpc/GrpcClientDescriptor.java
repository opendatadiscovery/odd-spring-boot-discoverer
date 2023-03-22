package org.opendatadiscovery.discoverer.model.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class GrpcClientDescriptor {
    private final String clientName;
    private final ServiceDescriptor serviceDescriptor;
    private final Map<String, MethodDescriptor<?, ?>> whitelistedMethodDescriptors;

    public GrpcClientDescriptor(final String clientName,
                                final ServiceDescriptor serviceDescriptor,
                                final Collection<MethodDescriptor<?, ?>> whitelistedMethodDescriptors) {
        this.serviceDescriptor = requireNonNull(serviceDescriptor, "Service descriptor cannot be null");
        this.clientName = requireNonNull(clientName, "Client name cannot be null");
        this.whitelistedMethodDescriptors = prepareMethodDescriptors(whitelistedMethodDescriptors);
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

    private Map<String, MethodDescriptor<?, ?>> prepareMethodDescriptors(
        final Collection<MethodDescriptor<?, ?>> whitelistedMethodDescriptors
    ) {
        if (whitelistedMethodDescriptors == null || whitelistedMethodDescriptors.isEmpty()) {
            return Collections.emptyMap();
        }

        return whitelistedMethodDescriptors
            .stream()
            .collect(Collectors.toMap(MethodDescriptor::getBareMethodName, identity()));
    }
}
