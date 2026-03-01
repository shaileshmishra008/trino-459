/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.oci;


import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.oci.OciFileSystemConfig.AuthType;
import io.trino.filesystem.oci.OciFileSystemConfig.CONN_CLOSING_STRATEGY;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.glassfish.jersey.apache.connector.ApacheConnectionClosingStrategy;

import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.http.ClientConfigurator;
import com.oracle.bmc.http.client.HttpClientBuilder;
import com.oracle.bmc.http.client.StandardClientProperties;
import com.oracle.bmc.http.client.jersey3.ApacheClientProperties;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

import com.oracle.bmc.objectstorage.ObjectStorageClient;

public class OciFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final ObjectStorageClient objectStorageClient;
    private final Executor uploadExecutor;
    @Inject
    public OciFileSystemFactory(OpenTelemetry openTelemetry, OciFileSystemConfig config)
    {
        this.readBlockSize = requireNonNull(config.getReadBlockSize(), "readBlockSize is null");
        this.writeBlockSize = requireNonNull( config.getWriteBlockSize(), "writeBlockSize is null");
        checkArgument(config.getMaxWriteConcurrency() >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = config.getMaxWriteConcurrency();
        this.maxSingleUploadSize = requireNonNull(config.getMaxSingleUploadSize(), "maxSingleUploadSize is null");

        final PoolingHttpClientConnectionManager poolConnectionManager =
                new PoolingHttpClientConnectionManager();
        poolConnectionManager.setMaxTotal(config.getConnPoolSize());
        poolConnectionManager.setDefaultMaxPerRoute(config.getConnPoolSize());


        this.objectStorageClient = ObjectStorageClient.builder().clientConfigurator(builder -> {
            builder.property(
                    StandardClientProperties.BUFFER_REQUEST, false);
            builder.property(
                    ApacheClientProperties.CONNECTION_MANAGER,
                    poolConnectionManager);
            builder.property(ApacheClientProperties.CONNECTION_CLOSING_STRATEGY,
                    config.getConnClosingStrategy() == CONN_CLOSING_STRATEGY.GRACEFUL_CLOSING_STRATEGY ? new ApacheConnectionClosingStrategy.GracefulClosingStrategy() : new ApacheConnectionClosingStrategy.ImmediateClosingStrategy());
            builder.property(StandardClientProperties.READ_TIMEOUT, Duration.ofMillis(config.getReadTimeOutMillis()));
            builder.property(StandardClientProperties.CONNECT_TIMEOUT, Duration.ofMillis(config.getConnTimeOutMillis()));

        }).region(config.getRegion()).build(config.getAuthType() == AuthType.CUSTOM_RESOURCE_PRINCIPAL ? ResourcePrincipalAuthenticationDetailsProvider.builder().build() : InstancePrincipalsAuthenticationDetailsProvider.builder().build());
        this.uploadExecutor =  newCachedThreadPool(daemonThreadsNamed("oci-upload-%s"));
    }

    @PreDestroy
    public void destroy()
    {
      this.objectStorageClient.close();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new OciFileSystem(this.uploadExecutor, this.objectStorageClient);
    }
}
