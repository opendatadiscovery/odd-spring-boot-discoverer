package org.opendatadiscovery.discoverer.impl;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.MethodDescriptor;
import io.grpc.NameResolver;
import io.grpc.NameResolverRegistry;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.internal.GrpcUtil;
import net.devh.boot.grpc.client.config.GrpcChannelProperties;
import net.devh.boot.grpc.client.config.GrpcChannelsProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.opendatadiscovery.discoverer.PathDiscoverer;
import org.opendatadiscovery.discoverer.model.Paths;
import org.opendatadiscovery.discoverer.model.grpc.GrpcClientDescriptor;
import org.opendatadiscovery.discoverer.model.grpc.GrpcClientDescriptorRegistry;
import org.opendatadiscovery.oddrn.model.GrpcServicePath;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class GrpcClientPathDiscoverer implements PathDiscoverer {
    private static final Log LOG = LogFactory.getLog(GrpcClientPathDiscoverer.class);
    private static final SynchronizationContext SYNC_CONTEXT = new SynchronizationContext((t, e) -> {
        throw new AssertionError(e);
    });

    private static final NameResolver.Args DEFAULT_NAME_RESOLVER_ARGUMENTS = NameResolver.Args.newBuilder()
        .setDefaultPort(9090)
        .setProxyDetector(GrpcUtil.NOOP_PROXY_DETECTOR)
        .setSynchronizationContext(SYNC_CONTEXT)
        .setServiceConfigParser(new NameResolverServiceConfigParser())
        .build();

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
                clientAddress = extractAddressForClient(clientDescriptor.getClientName()).get();
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

    private CompletableFuture<String> extractAddressForClient(final String clientName) {
        final GrpcChannelProperties clientChannel = channelsProperties.getChannel(clientName);

        if (clientChannel == null) {
            throw new IllegalArgumentException(
                String.format("No channel properties for client %s were found", clientName));
        }

        final URI clientChannelAddress = clientChannel.getAddress();
        if (clientChannelAddress == null) {
            throw new IllegalArgumentException(String.format("No address for client %s was found", clientName));
        }

        return resolveURI(clientChannelAddress);
    }

    private CompletableFuture<String> resolveURI(final URI uri) {
        final CompletableFuture<String> resultFuture = new CompletableFuture<>();

        NameResolverRegistry.getDefaultRegistry()
            .asFactory()
            .newNameResolver(uri, DEFAULT_NAME_RESOLVER_ARGUMENTS)
            .start(new NameResolverListener(resultFuture));

        return resultFuture;
    }

    private static final class NameResolverServiceConfigParser extends NameResolver.ServiceConfigParser {
        @Override
        public NameResolver.ConfigOrError parseServiceConfig(final Map<String, ?> rawServiceConfig) {
            return null;
        }
    }

    private static final class NameResolverListener implements io.grpc.NameResolver.Listener {
        private final CompletableFuture<String> future;

        private NameResolverListener(final CompletableFuture<String> future) {
            this.future = future;
        }

        @Override
        public void onAddresses(final List<EquivalentAddressGroup> servers, final Attributes attributes) {
            final String address = servers.stream()
                .flatMap(s -> s.getAddresses().stream())
                .filter(InetSocketAddress.class::isInstance)
                .map(a -> ((InetSocketAddress) a).getHostName())
                .distinct()
                .sorted()
                .collect(Collectors.joining(","));

            future.complete(address);
        }

        @Override
        public void onError(final Status error) {
            future.completeExceptionally(error.asException());
        }
    }
}