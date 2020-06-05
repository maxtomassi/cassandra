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

import com.google.common.collect.Iterables;

/**
 * An immutable representation of a full schema.
 *
 * <p>Note that this only represent non-local keyspace/tables, so does not include local system keyspace, nor virtual
 * ones. Those are handled separately in {@link SchemaManager} though {@link LocalKeyspaces}.
 *
 * <p>Also note that this represent an "applied" schema, one that is either the current schema or previously was, and
 * in particular exposes the version identifying said schema through {@link #versionAsUUID()}.
 */
public abstract class Schema
{
    final Keyspaces keyspaces;

    Schema(Keyspaces keyspaces)
    {
        this.keyspaces = keyspaces;
    }

    /**
     * The UUID version identifying the schema.
     *
     * <p>The property of the version is that version equality is equivalent to schema equality.
     */
    public abstract UUID versionAsUUID();

    /**
     * Checks whether this schema is empty, where empty is the definition described on {@link SchemaManager#isEmpty()}.
     */
    public boolean isEmpty()
    {
        return keyspaces.isEmpty();
    }

    /**
     * The actual keyspaces definitions that compose this schema.
     */
    public Keyspaces keyspaces()
    {
        return keyspaces;
    }

    /**
     * The total number of tables defined in this schema.
     */
    int numberOfTables()
    {
        return keyspaces.stream().mapToInt(k -> Iterables.size(k.tablesAndViews())).sum();
    }

    @Override
    public String toString()
    {
        return String.format("[%s]=%s", versionAsUUID(), keyspaces);
    }
}