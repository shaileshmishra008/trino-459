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

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputStream;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

final class OciInputStream
        extends TrinoInputStream
{
    private static final int MAX_SKIP_BYTES = 1024 * 1024;

    private final Location location;
    private final ObjectStorageClient client;
    private final GetObjectRequest request;
    private final Long length;

    private boolean closed;
    private GetObjectResponse response;
    private InputStream in;
    private long streamPosition;
    private long nextReadPosition;

    public OciInputStream(Location location, ObjectStorageClient client, GetObjectRequest request, Long length)
    {
        this.location = requireNonNull(location, "location is null");
        this.client = requireNonNull(client, "client is null");
        this.request = requireNonNull(request, "request is null");
        this.length = length;
    }

    @Override
    public int available()
            throws IOException
    {
        ensureOpen();
        if ((in != null) && (nextReadPosition == streamPosition)) {
            return getAvailable();
        }
        return 0;
    }

    @Override
    public long getPosition()
    {
        return nextReadPosition;
    }

    @Override
    public void seek(long position)
            throws IOException
    {
        ensureOpen();
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if ((length != null) && (position > length)) {
            throw new IOException("Cannot seek to %s. File size is %s: %s".formatted(position, length, location));
        }

        nextReadPosition = position;
    }

    @Override
    public int read()
            throws IOException
    {
        ensureOpen();
        seekStream();

        int value = doRead();
        if (value >= 0) {
            streamPosition++;
            nextReadPosition++;
        }
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();
        seekStream();

        int n = doRead(bytes, offset, length);
        if (n > 0) {
            streamPosition += n;
            nextReadPosition += n;
        }
        return n;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        ensureOpen();
        seekStream();

        long skip = doSkip(n);
        streamPosition += skip;
        nextReadPosition += skip;
        return skip;
    }

    @Override
    public void skipNBytes(long n)
            throws IOException
    {
        ensureOpen();

        if (n <= 0) {
            return;
        }

        long position = nextReadPosition + n;
        if ((position < 0) || (length != null && position > length)) {
            throw new EOFException("Unable to skip %s bytes (position=%s, fileSize=%s): %s".formatted(n, nextReadPosition, length, location));
        }
        nextReadPosition = position;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        closeStream();
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Input stream closed: " + location);
        }
    }

    private void seekStream()
            throws IOException
    {
        if ((in != null) && (nextReadPosition == streamPosition)) {
            // already at specified position
            return;
        }

        if ((in != null) && (nextReadPosition > streamPosition)) {
            // seeking forwards
            long skip = nextReadPosition - streamPosition;
            if (skip <= max(getAvailable(), MAX_SKIP_BYTES)) {
                // already buffered or seek is small enough
                if (doSkip(skip) == skip) {
                    streamPosition = nextReadPosition;
                    return;
                }
            }
        }

        // close the stream and open at desired position
        streamPosition = nextReadPosition;
        closeStream();

        try {
            GetObjectRequest rangeRequest = request;
            if (nextReadPosition != 0) {
                String range = "bytes=%s-".formatted(nextReadPosition);
                rangeRequest = request.toBuilder().range(new Range(nextReadPosition, length -1)).build();
            }
            response = client.getObject(rangeRequest);
            in = response.getInputStream();
            if (response.getContentLength() != null && response.getContentLength() == 0) {
                in = nullInputStream();
            }
            streamPosition = nextReadPosition;
        }
        catch (BmcException e) {
            if(e.getStatusCode() == 404) {
                var ex = new FileNotFoundException(location.toString());
                ex.initCause(e);
                throw ex;
            }
            throw new TrinoFileSystemException("Failed to open S3 file: " + location, e);
        }
    }

    private void closeStream()
    {
        if (in == null) {
            return;
        }

        try (var _ = in) {
            in.close();
        }
        catch (IOException _) {
        }
        finally {
            in = null;
        }
    }

    private int getAvailable()
            throws IOException
    {

            return in.available();

    }

    private long doSkip(long n)
            throws IOException
    {

            return in.skip(n);

    }

    private int doRead()
            throws IOException
    {

            return in.read();

    }

    private int doRead(byte[] bytes, int offset, int length)
            throws IOException
    {

            return in.read(bytes, offset, length);

    }
}
