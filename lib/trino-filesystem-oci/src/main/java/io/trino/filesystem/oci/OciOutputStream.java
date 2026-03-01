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

import static java.lang.Math.clamp;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.AbortMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

final class OciOutputStream
        extends OutputStream
{
    List<Integer> parts = new ArrayList<>();
    private final LocalMemoryContext memoryContext;

    private final ObjectStorageClient client;
    private final OciLocation location;

    private final int partSize;


    private int currentPartNumber;
    private byte[] buffer = new byte[0];
    private int bufferSize;
    private int initialBufferSize = 64;

    private boolean closed;
    private boolean failed;
    private boolean multipartUploadStarted;
    private Future<Integer> inProgressUploadFuture;
    private final Executor uploadExecutor;
    // Mutated by background thread which does the multipart upload.
    // Read by both main thread and background thread.
    // Visibility is ensured by calling get() on inProgressUploadFuture.
    private Optional<String> uploadId = Optional.empty();

    public OciOutputStream(AggregatedMemoryContext memoryContext, Executor uploadExecutor, ObjectStorageClient client, OciLocation location)
    {
        this.memoryContext = memoryContext.newLocalMemoryContext(OciOutputStream.class.getSimpleName());
        this.client = requireNonNull(client, "client is null");
        this.location = requireNonNull(location, "location is null");
        //TODO: make it configurable
        this.partSize = 5 * 1024 * 1024;
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");

    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        ensureCapacity(1);
        buffer[bufferSize] = (byte) b;
        bufferSize++;
        flushBuffer(false);
    }

    @Override
    public void write(byte[] bytes, int offset, int length)
            throws IOException
    {
        ensureOpen();

        while (length > 0) {
            ensureCapacity(length);

            int copied = min(buffer.length - bufferSize, length);
            arraycopy(bytes, offset, buffer, bufferSize, copied);
            bufferSize += copied;

            flushBuffer(false);

            offset += copied;
            length -= copied;
        }
    }

    @Override
    public void flush()
            throws IOException
    {
        ensureOpen();
        flushBuffer(false);
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        if (failed) {
            try {
                abortUpload();
                return;
            }
            catch (BmcException e) {
                throw new IOException(e);
            }
        }

        try {
            flushBuffer(true);
            memoryContext.close();
            waitForPreviousUploadFinish();
        }
        catch (IOException | RuntimeException e) {
            abortUploadSuppressed(e);
            throw e;
        }

        try {
            uploadId.ifPresent(this::finishUpload);
        }
        catch (BmcException e) {
            abortUploadSuppressed(e);
            throw new IOException(e);
        }
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    private void ensureCapacity(int extra)
    {
        int capacity = min(partSize, bufferSize + extra);
        if (buffer.length < capacity) {
            int target = max(buffer.length, initialBufferSize);
            if (target < capacity) {
                target += target / 2; // increase 50%
                target = clamp(target, capacity, partSize);
            }
            buffer = Arrays.copyOf(buffer, target);
            memoryContext.setBytes(buffer.length);
        }
    }

    private void flushBuffer(boolean finished)
            throws IOException
    {
        // skip multipart upload if there would only be one part
        if (finished && !multipartUploadStarted) {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucketName(location.bucket())
                    .namespaceName(location.namespace())
                    .objectName(location.key())
                    .contentLength((long) bufferSize)
                    .putObjectBody(new ByteArrayInputStream(ByteBuffer.wrap(buffer, 0, bufferSize).array()))
                    .build();
            try {
                client.putObject(request);
                return;
            }
            catch (BmcException e) {
                failed = true;
                throw new IOException("Put failed for bucket [%s] key [%s]: %s".formatted(location.bucket(), location.key(), e), e);
            }
        }

        // the multipart upload API only allows the last part to be smaller than 5MB
        if ((bufferSize == partSize) || (finished && (bufferSize > 0))) {
            byte[] data = buffer;
            int length = bufferSize;

            if (finished) {
                this.buffer = null;
            }
            else {
                this.buffer = new byte[0];
                this.initialBufferSize = partSize;
                bufferSize = 0;
            }
            memoryContext.setBytes(0);

            try {
                waitForPreviousUploadFinish();
            }
            catch (IOException e) {
                failed = true;
                abortUploadSuppressed(e);
                throw e;
            }
            multipartUploadStarted = true;
            inProgressUploadFuture = supplyAsync(() -> uploadPage(data, length), uploadExecutor);
        }
    }

    private void waitForPreviousUploadFinish()
            throws IOException
    {
        if (inProgressUploadFuture == null) {
            return;
        }

        try {
            inProgressUploadFuture.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        catch (ExecutionException e) {
            throw new IOException("Streaming upload failed", e);
        }
    }

    private Integer uploadPage(byte[] data, int length)
    {
        if (uploadId.isEmpty()) {
            CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                    .bucketName(location.bucket()).namespaceName(location.namespace())
                    .build();

            uploadId = Optional.of(client.createMultipartUpload(request).getMultipartUpload().getUploadId());
        }

        currentPartNumber++;
        UploadPartRequest request = UploadPartRequest.builder()
                .bucketName(location.bucket())
                .namespaceName(location.namespace())
                .objectName(location.key())
                .contentLength((long) length)
                .uploadId(uploadId.get())
                .uploadPartNum(currentPartNumber)
                .uploadPartBody(new ByteArrayInputStream(ByteBuffer.wrap(data, 0, length).array()))
                .build();

        UploadPartResponse response = client.uploadPart(request);
        parts.add(currentPartNumber);
        return currentPartNumber;
    }

    private void finishUpload(String uploadId)
    {
        CommitMultipartUploadRequest request = CommitMultipartUploadRequest.builder()
                .bucketName(location.bucket())
                .namespaceName(location.namespace())
                .objectName(location.key())
                .uploadId(uploadId)
                .build();

        client.commitMultipartUpload(request);
    }

    private void abortUpload()
    {
        uploadId.map(id -> AbortMultipartUploadRequest.builder()
                        .bucketName(location.bucket())
                        .namespaceName(location.namespace())
                        .objectName(location.key())
                        .uploadId(id)
                        .build())
                .ifPresent(client::abortMultipartUpload);
    }

    @SuppressWarnings("ObjectEquality")
    private void abortUploadSuppressed(Throwable throwable)
    {
        try {
            abortUpload();
        }
        catch (Throwable t) {
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}
