package org.opendatadiscovery.discoverer.impl;

import io.grpc.MethodDescriptor;
import net.devh.boot.grpc.client.config.GrpcChannelProperties;
import net.devh.boot.grpc.client.config.GrpcChannelsProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.discoverer.model.grpc.GrpcClientDescriptor;
import org.opendatadiscovery.discoverer.model.grpc.GrpcClientDescriptorRegistry;
import org.opendatadiscovery.oddrn.model.GrpcServicePath;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class GrpcClientPathDiscoverer implements PathDiscoverer {
    private static final Log LOG = LogFactory.getLog(GrpcClientPathDiscoverer.class);

    private final GrpcChannelsProperties channelsProperties;
    private final GrpcClientDescriptorRegistry clientDescriptorRegistry;

    public GrpcClientPathDiscoverer(final GrpcChannelsProperties channelsProperties,
                                    final GrpcClientDescriptorRegistry clientDescriptorRegistry) {
        this.channelsProperties = channelsProperties;
        this.clientDescriptorRegistry = clientDescriptorRegistry;
    }

    @Override
    public Paths discover() {
        final Set<GrpcServicePath> grpcServicePaths = new HashSet<>();

        for (final GrpcClientDescriptor clientDescriptor : this.clientDescriptorRegistry.getClientDescriptors()) {
            final boolean discoverAllMethods = clientDescriptor.getWhitelistedMethodDescriptors().isEmpty();

            final String clientAddress;
            try {
                clientAddress = extractAddressForClient(clientDescriptor.getClientName());
            } catch (final Exception e) {
                LOG.error(e.getMessage(), e);
                continue;
            }

            for (final MethodDescriptor<?, ?> methodDescriptor : clientDescriptor.getServiceDescriptor().getMethods()) {
                if (discoverAllMethods || clientDescriptor.isMethodWhitelisted(methodDescriptor.getBareMethodName())) {
                    grpcServicePaths.add(GrpcServicePath.builder()
                        .method(methodDescriptor.getBareMethodName())
                        .service(clientDescriptor.getServiceDescriptor().getName())
                        .host(clientAddress)
                        .build());
                }
            }
        }

        return new Paths(Collections.emptySet(), grpcServicePaths);
    }

    private String extractAddressForClient(final String clientName) {
        final GrpcChannelProperties clientChannel = channelsProperties.getChannel(clientName);

        if (clientChannel == null) {
            throw new IllegalArgumentException(
                String.format("No channel properties for client %s were found", clientName));
        }

        final URI clientChannelAddress = clientChannel.getAddress();
        if (clientChannelAddress == null) {
            throw new IllegalArgumentException(String.format("No address for client %s was found", clientName));
        }

        final String authority = clientChannelAddress.getAuthority();
        final String[] authoritySplitted = authority.split(":");

        if (authoritySplitted.length == 1) {
            return authority;
        }

        return authoritySplitted[0];
    }
}
