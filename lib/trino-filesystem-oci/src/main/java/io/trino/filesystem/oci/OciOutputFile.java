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

import static java.util.Objects.requireNonNull;

import com.oracle.bmc.objectstorage.ObjectStorageClient;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;

final class OciOutputFile
        implements TrinoOutputFile
{
    private final Executor uploadExecutor;
    private final ObjectStorageClient client;
    private final OciLocation location;

    public OciOutputFile(Executor uploadExecutor, ObjectStorageClient client, OciLocation location)
    {
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try (OutputStream out = create()) {
            out.write(data);
        }
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
    {
        return new OciOutputStream(memoryContext,this.uploadExecutor, client, location);
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
