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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import jakarta.validation.constraints.NotNull;
import org.glassfish.jersey.apache.connector.ApacheConnectionClosingStrategy;

public class OciFileSystemConfig
{
    public enum AuthType
    {
        INSTANCE_PRINCIPAL,
        CUSTOM_RESOURCE_PRINCIPAL,
    }

    public enum  CONN_CLOSING_STRATEGY
    {
        IMMEDIATE_CLOSING_STRATEGY,
        GRACEFUL_CLOSING_STRATEGY,
    }

    private AuthType authType = AuthType.INSTANCE_PRINCIPAL;

    private DataSize readBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private int maxWriteConcurrency = 8;
    private DataSize maxSingleUploadSize = DataSize.of(4, Unit.MEGABYTE);
    private int readTimeOutMillis = 60000;
    private int connTimeOutMillis = 10000;
    private String region;
    private int connPoolSize = 50;
    private CONN_CLOSING_STRATEGY connectionClosingStrategy = CONN_CLOSING_STRATEGY.IMMEDIATE_CLOSING_STRATEGY;

    public CONN_CLOSING_STRATEGY getConnClosingStrategy()
    {
        return connectionClosingStrategy;
    }

    @Config("oci.conn-closing-strategy")
    public OciFileSystemConfig setConnClosingStrategy(CONN_CLOSING_STRATEGY connectionClosingStrategy)
    {
        this.connectionClosingStrategy = connectionClosingStrategy;
        return this;
    }

    public int getConnPoolSize()
    {
        return connPoolSize;
    }

    @Config("oci.conn-pool-size")
    public OciFileSystemConfig setConnPoolSize(int connPoolSize)
    {
        this.connPoolSize = connPoolSize;
        return this;
    }

    @NotNull
    public String getRegion()
    {
        return region;
    }

    @Config("oci.region")
    public OciFileSystemConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public int getConnTimeOutMillis()
    {
        return connTimeOutMillis;
    }

    @Config("oci.conn-timeout-millis")
    public OciFileSystemConfig setConnTimeOutMillis(int connTimeOutMillis)
    {
        this.connTimeOutMillis = connTimeOutMillis;
        return this;
    }

    public int getReadTimeOutMillis()
    {
        return readTimeOutMillis;
    }

    @Config("oci.read-timeout-millis")
    public OciFileSystemConfig setReadTimeOutMillis(int readTimeOutMillis)
    {
        this.readTimeOutMillis = readTimeOutMillis;
        return this;
    }

    @NotNull
    public AuthType getAuthType()
    {
        return authType;
    }

    @Config("oci.auth-type")
    public OciFileSystemConfig setAuthType(AuthType authType)
    {
        this.authType = authType;
        return this;
    }

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    @Config("oci.read-block-size")
    public OciFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    @Config("oci.write-block-size")
    public OciFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    public int getMaxWriteConcurrency()
    {
        return maxWriteConcurrency;
    }

    @Config("oci.max-write-concurrency")
    public OciFileSystemConfig setMaxWriteConcurrency(int maxWriteConcurrency)
    {
        this.maxWriteConcurrency = maxWriteConcurrency;
        return this;
    }

    @NotNull
    public DataSize getMaxSingleUploadSize()
    {
        return maxSingleUploadSize;
    }

    @Config("oci.max-single-upload-size")
    public OciFileSystemConfig setMaxSingleUploadSize(DataSize maxSingleUploadSize)
    {
        this.maxSingleUploadSize = maxSingleUploadSize;
        return this;
    }
}
