package org.opendatadiscovery.discoverer.model.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class GrpcClientDescriptorRegistry {
    private final List<GrpcClientDescriptor> clientDescriptors;

    public GrpcClientDescriptorRegistry(final List<GrpcClientDescriptor> clientDescriptors) {
        this.clientDescriptors = requireNonNull(clientDescriptors, "Client descriptors' list cannot be null");
    }

    public List<GrpcClientDescriptor> getClientDescriptors() {
        return clientDescriptors;
    }

    public static GrpcClientDescriptorRegistryBuilder builder() {
        return new GrpcClientDescriptorRegistryBuilder();
    }

    public static class GrpcClientDescriptorRegistryBuilder {
        private final List<GrpcClientDescriptor> clientDescriptors = new ArrayList<>();

        public GrpcClientDescriptorRegistryBuilder add(final String clientName,
                                                       final ServiceDescriptor serviceDescriptor) {
            return add(clientName, serviceDescriptor, null);
        }

        public GrpcClientDescriptorRegistryBuilder add(
            final String clientName,
            final ServiceDescriptor serviceDescriptor,
            final List<MethodDescriptor<?, ?>> whitelistedMethodDescriptors
        ) {
            clientDescriptors.add(
                new GrpcClientDescriptor(clientName, serviceDescriptor, whitelistedMethodDescriptors));

            return this;
        }

        public GrpcClientDescriptorRegistry build() {
            return new GrpcClientDescriptorRegistry(clientDescriptors);
        }
    }
}