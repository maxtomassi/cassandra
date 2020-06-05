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

import com.google.common.hash.Hashing;

/**
 * {@link Schema} concrete implementation for the "default" schema handling, the one handled by
 * {@link DefaultSchemaUpdateHandler}.
 *
 * <p>Default schema uses a version based on hashing the content of the schema tables in {@link SchemaKeyspace}.
 */
class DefaultSchema extends Schema
{
    static final UUID EMPTY_VERSION = UUID.nameUUIDFromBytes(Hashing.md5().newHasher()
                                                                          .hash()
                                                                          .asBytes());
    static final DefaultSchema EMPTY = new DefaultSchema(Keyspaces.none(), EMPTY_VERSION);

    private final UUID version;

    DefaultSchema(Keyspaces keyspaces, UUID version)
    {
        super(keyspaces);
        this.version = version;
    }

    @Override
    public UUID versionAsUUID()
    {
        return version;
    }

    @Override
    public boolean isEmpty()
    {
        return EMPTY_VERSION.equals(version);
    }
}