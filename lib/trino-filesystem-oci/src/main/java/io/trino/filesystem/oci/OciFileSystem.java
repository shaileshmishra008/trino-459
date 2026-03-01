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

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.partition;
import static com.google.common.collect.Multimaps.toMultimap;
import static java.util.Objects.requireNonNull;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.ObjectSummary;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListObjectsRequest;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemException;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

final class OciFileSystem
        implements TrinoFileSystem
{
    private final ObjectStorageClient client;
    private final Executor uploadExecutor;

    public OciFileSystem(Executor uploadExecutor, ObjectStorageClient client)
    {

        this.client = requireNonNull(client, "client is null");
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new OciInputFile(client, new OciLocation(location), null, null);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new OciInputFile(client, new OciLocation(location), length, null);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified) {
        return new OciInputFile(client, new OciLocation(location), length, lastModified);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new OciOutputFile(uploadExecutor, client, new OciLocation(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        location.verifyValidFileLocation();
        OciLocation ociLocation = new OciLocation(location);
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .objectName(ociLocation.key())
                .bucketName(ociLocation.bucket())
                .namespaceName(ociLocation.namespace())
                .build();

        try {
            client.deleteObject(request);
        }
        catch (BmcException e) {
            throw new TrinoFileSystemException("Failed to delete file: " + location, e);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        FileIterator iterator = listObjects(location, true);
        while (iterator.hasNext()) {
            List<Location> files = new ArrayList<>();
            while ((files.size() < 1000) && iterator.hasNext()) {
                files.add(iterator.next().location());
            }
            deleteObjects(files);
        }
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        locations.forEach(Location::verifyValidFileLocation);
        deleteObjects(locations);
    }

    private void deleteObjects(Collection<Location> locations)
            throws IOException
    {
        String namespace = locations.stream().findAny().map(l -> new OciLocation(l)).map(ociL -> ociL.namespace()).get();
        SetMultimap<String, String> bucketToKeys = locations.stream()
                .map(OciLocation::new)
                .collect(toMultimap(OciLocation::bucket, OciLocation::key, HashMultimap::create));

        Map<String, String> failures = new HashMap<>();

        for (Entry<String, Collection<String>> entry : bucketToKeys.asMap().entrySet()) {
            String bucket = entry.getKey();
            Collection<String> allKeys = entry.getValue();

            //TODO: bulk delete option in OCI??
            for (String key : allKeys) {
                /*List<ObjectIdentifier> objects = keys.stream()
                        .map(key -> ObjectIdentifier.builder().key(key).build())
                        .toList();*/

                DeleteObjectRequest  request = DeleteObjectRequest.builder()
                        .bucketName(bucket)
                         .namespaceName(namespace)
                         .objectName(key)
                        .build();

                try {
                    DeleteObjectResponse response = client.deleteObject(request);
                }
                catch (BmcException e) {
                    //throw new TrinoFileSystemException("Error while batch deleting files", e);
                    failures.put("oci://%s@%s/%s".formatted(bucket,namespace, key), ""+e.getStatusCode());
                }
            }
        }

        if (!failures.isEmpty()) {
            throw new IOException("Failed to delete one or more files: " + failures);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        throw new IOException("Oci does not support renames");
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return listObjects(location, false);
    }

    private FileIterator listObjects(Location location, boolean includeDirectoryObjects)
            throws IOException
    {
        OciLocation ociLocation = new OciLocation(location);

        String key = ociLocation.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsRequest request = ListObjectsRequest.builder()
                .namespaceName(ociLocation.namespace())
                .bucketName(ociLocation.bucket())
                .prefix(key)
                .fields("name,size,timeModified")
                .build();

        try {
            Stream<ObjectSummary> ociObjectStream = client.listObjects(request).getListObjects().getObjects().stream();
            if (!includeDirectoryObjects) {
                ociObjectStream = ociObjectStream.filter(object -> !object.getName().endsWith("/"));
            }
            return new OciFileIterator(ociLocation, ociObjectStream.iterator());
        }
        catch (BmcException e) {
            throw new TrinoFileSystemException("Failed to list location: " + location, e);
        }
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        validateOciLocation(location);
        if (location.path().isEmpty() || listFiles(location).hasNext()) {
            return Optional.of(true);
        }
        return Optional.empty();
    }

    @Override
    public void createDirectory(Location location)
    {
        validateOciLocation(location);
        // Oci does not have directories
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        throw new IOException("Oci does not support directory renames");
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        OciLocation ociLocation = new OciLocation(location);
        Location baseLocation = ociLocation.baseLocation();

        String key = ociLocation.key();
        if (!key.isEmpty() && !key.endsWith("/")) {
            key += "/";
        }

        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucketName(ociLocation.bucket())
                .namespaceName(ociLocation.namespace())
                .prefix(key)
                .delimiter("/")
                .build();

        try {
            return client.listObjects(request).getListObjects().getObjects()
                    .stream()
                    .map(ObjectSummary::getName)
                    .map(baseLocation::appendPath)
                    .collect(toImmutableSet());
        }
        catch (BmcException e) {
            throw new TrinoFileSystemException("Failed to list location: " + location, e);
        }
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
    {
        validateOciLocation(targetPath);
        // Oci does not have directories
        return Optional.empty();
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    private static void validateOciLocation(Location location)
    {
        new OciLocation(location);
    }
}
