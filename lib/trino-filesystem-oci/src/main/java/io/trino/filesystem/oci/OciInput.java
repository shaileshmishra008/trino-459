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

import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.HeadObjectRequest;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInput;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.OptionalLong;

final class OciInput
        implements TrinoInput
{
    private final Location location;
    private final ObjectStorageClient client;
    private final GetObjectRequest request;
    private boolean closed;
    private OptionalLong objectLength = OptionalLong.empty();

    public OciInput(Location location, ObjectStorageClient client, GetObjectRequest request)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return;
        }

        GetObjectRequest rangeRequest = request.toBuilder().range(new Range(position, ((position + length) - 1))).build();

        int n = read(buffer, offset, length, rangeRequest);
        if (n < length) {
            throw new EOFException("Read %s of %s requested bytes: %s".formatted(n, length, location));
        }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length)
            throws IOException {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (length == 0) {
            return 0;
        }

        try {
            if (objectLength.isEmpty()) {
                HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                        .bucketName(request.getBucketName())
                        .namespaceName(request.getNamespaceName())
                        .objectName(request.getObjectName())
                        .build();
                objectLength = OptionalLong.of(this.client.headObject(headObjectRequest).getContentLength());
            }

            GetObjectRequest rangeRequest = request.toBuilder().range(new Range((objectLength.orElseThrow() - length),
                    (objectLength.orElseThrow()-1))).build();

            return read(buffer, offset, length, rangeRequest);
        }catch (RuntimeException e) {
            throw new IOException("reading file : "+location, e);
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input closed: " + location);
        }
    }

    private int read(byte[] buffer, int offset, int length, GetObjectRequest rangeRequest)
            throws IOException
    {


                try {
                    return client.getObject(rangeRequest).getInputStream().readNBytes(buffer, offset, length);
                }
                catch (IOException e) {
                    //TODO: we can retry this error
                    throw new TrinoFileSystemException("Error reading file: " + location, e);
                }
    }
}
