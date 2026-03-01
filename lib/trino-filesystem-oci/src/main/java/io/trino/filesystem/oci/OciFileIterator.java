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
import static com.google.common.base.Verify.verify;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.model.ObjectSummary;

import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

final class OciFileIterator
        implements FileIterator
{
    private final OciLocation location;
    private final Iterator<ObjectSummary> iterator;
    private final Location baseLocation;

    public OciFileIterator(OciLocation location, Iterator<ObjectSummary> iterator)
    {
        this.location = requireNonNull(location, "location is null");
        this.iterator = requireNonNull(iterator, "iterator is null");
        this.baseLocation = location.baseLocation();
    }

    @Override
    public boolean hasNext()
            throws IOException
    {
        try {
            return iterator.hasNext();
        }
        catch (BmcException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }

    @Override
    public FileEntry next()
            throws IOException
    {
        try {
            ObjectSummary object = iterator.next();
            System.out.println(object.toString());
            verify(object.getName().startsWith(location.key()), "OCI listed key [%s] does not start with prefix [%s]", object.getName(), location.key());

            return new FileEntry(
                    baseLocation.appendPath(object.getName()),
                    object.getSize() == null ? 0L : object.getSize() ,
                    object.getTimeModified().toInstant(),
                    Optional.empty(),
                    ImmutableSet.of());
        }
        catch (BmcException e) {
            throw new IOException("Failed to list location: " + location, e);
        }
    }
}
