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

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest;
import com.oracle.bmc.objectstorage.responses.HeadObjectResponse;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;

final class OciInputFile
        implements TrinoInputFile
{
    private final ObjectStorageClient client;
    private final OciLocation location;
    private Long length;
    private Instant lastModified;

    public OciInputFile(ObjectStorageClient client, OciLocation location, Long length, Instant lastModified)
    {
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        this.length = length;
        this.lastModified = lastModified;
        location.location().verifyValidFileLocation();
    }

    @Override
    public TrinoInput newInput()
    {
        return new OciInput(location(), client, newGetObjectRequest());
    }

    @Override
    public TrinoInputStream newStream()
    {
        return new OciInputStream(location(), client, newGetObjectRequest(), length);
    }

    @Override
    public long length()
            throws IOException
    {
        if ((length == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if ((lastModified == null) && !headObject()) {
            throw new FileNotFoundException(location.toString());
        }
        return lastModified;
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return headObject();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private GetObjectRequest newGetObjectRequest()
    {
        return GetObjectRequest.builder()
                .bucketName(location.bucket())
                .namespaceName(location.namespace())
                .objectName(location.key())
                .build();
    }

    private boolean headObject()
            throws IOException
    {
        HeadObjectRequest request = HeadObjectRequest.builder()
                .bucketName(location.bucket())
                .namespaceName(location.namespace())
                .objectName(location.key())
                .build();

        try {
            HeadObjectResponse response = client.headObject(request);
            if (length == null) {
                length = response.getContentLength();
            }
            if (lastModified == null) {
                lastModified = response.getLastModified().toInstant();
            }
            return true;
        }
        catch (BmcException e) {
            e.printStackTrace();
            if(e.getStatusCode() == 404) return false;
            throw new TrinoFileSystemException("OCI HEAD request failed for file: " + location, e);
        }
    }
}
