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

import com.google.common.base.CharMatcher;
import io.trino.filesystem.Location;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record OciLocation(Location location)
{
    OciLocation
    {
        requireNonNull(location, "location is null");
        checkArgument(location.scheme().isPresent(), "No scheme for location: %s", location);
        checkArgument("oci".equalsIgnoreCase(location.scheme().get()), "Wrong scheme for OCI location: %s", location);
        checkArgument(location.userInfo().isPresent(), "No bucket for OCI location: %s", location);
        checkArgument(location.host().isPresent(), "no namespace for OCI location: %s", location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String bucket()
    {
        return location.userInfo().orElseThrow();
    }

    public String key()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    public Location baseLocation()
    {
        return Location.of("%s://%s@%s/".formatted(scheme(), bucket(), namespace()));
    }

    public String namespace() {
        return location.host().orElseThrow();
    }

    public static void main(String[] args) throws Exception {
        Location loc = Location.of("oci://u7fwxvlobn6dpb4wp5zq_1726199289594_ms@axetxrhv8myo/managed/shaimish_demo_cat.cat/trino_demo_db.db/order");
        System.out.println(loc.scheme() + " "+loc.userInfo().isEmpty() + " " + loc.host() + " " + loc.path());
    }
}
