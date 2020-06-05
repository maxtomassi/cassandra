/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.schema;

import java.util.UUID;

import org.apache.cassandra.utils.UUIDGen;

/**
 * {@link Schema} concrete implementation used when offline, which is handled by {@link OfflineSchemaUpdateHandler}.
 */
class OfflineSchema extends Schema
{
    static final OfflineSchema EMPTY = new OfflineSchema(Keyspaces.none(), 0);

    /**
     * In theory, the version shouldn't matter for offline schema ({@link OfflineSchemaUpdateHandler} makes
     * sure its {@link OfflineSchemaUpdateHandler#announceVersionUpdate(UUID)} method does nothing), but to avoid
     * breaking code too easily, we still use a simple integer to represent the schema, which is incremented on every
     * change (by {@link OfflineSchemaUpdateHandler}).
     */
    long version;

    OfflineSchema(Keyspaces keyspaces, long version)
    {
        super(keyspaces);
        this.version = version;
    }

    @Override
    public UUID versionAsUUID()
    {
        return UUIDGen.getTimeUUID(version);
    }
}