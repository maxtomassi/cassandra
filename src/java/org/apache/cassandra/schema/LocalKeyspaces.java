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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.db.virtual.VirtualTable;

/**
 * Maintains the definition of local keyspaces (local system keyspaces and virtual ones) for {@link SchemaManager}.
 */
public class LocalKeyspaces
{
    private final SchemaManager schemaManager;

    private final Map<String, KeyspaceMetadata> localSystemKeyspaces = new ConcurrentHashMap<>();
    private final Map<TableId, TableMetadata> localSystemTables = new ConcurrentHashMap<>();

    private final Map<String, VirtualKeyspace> virtualKeyspaces = new ConcurrentHashMap<>();
    private final Map<TableId, VirtualTable> virtualTables = new ConcurrentHashMap<>();

    LocalKeyspaces(SchemaManager schemaManager)
    {
        this.schemaManager = schemaManager;
    }

    /**
     * Adds the hard-coded definitions for all local system keyspaces and virtual keyspaces.
     * <p>
     * This isn't done in the constructor mainly because this part is skipped when running in 'client' mode.
     */
    // TODO: this is public only because CQLSStableWriter needs those to be loaded, but it uses "client" initialization
    //   which doesn't call this. It's a bit unclear why CQLSSTableWriter needs this. Overall, this should probably
    //   not be public on principle, but needs some investigation.
    public void loadHardCodedDefinitions()
    {
        load(SchemaKeyspace.metadata());
        load(SystemKeyspace.metadata());

        load(VirtualSchemaKeyspace.instance);
        load(SystemViewsKeyspace.instance);
    }

    /**
     * Whether the provided keyspace name corresponds to an existing virtual keyspace.
     */
    public boolean isVirtualKeyspace(String keyspaceName)
    {
        return virtualKeyspaces.containsKey(keyspaceName);
    }

    /**
     * Returns the provided virtual table instance.
     *
     * @param table the virtual table to retrieve.
     * @return the virtual table instance of {@code table}, or {@code null} if either {@code table} is not a virtual
     * table or it isn't loaded.
     */
    @Nullable
    public VirtualTable virtualTableInstance(TableMetadata table)
    {
        return virtualTables.get(table.id);
    }

    /**
     * Gets the keyspace metadata for a local system keyspace or a virtual one.
     */
    KeyspaceMetadata getKeyspaceMetadata(String name)
    {
        KeyspaceMetadata metadata = getLocalSystemKeyspaceMetadata(name);
        return metadata == null ? getVirtualKeyspaceMetadata(name) : metadata;
    }

    /**
     * Gets the table metadata by ID for a local system table or a virtual one.
     */
    TableMetadata getTableMetadata(TableId id)
    {
        TableMetadata localTable = localSystemTables.get(id);
        if (localTable != null)
            return localTable;

        VirtualTable virtualTable = virtualTables.get(id);
        return virtualTable == null ? null : virtualTable.metadata();
    }

    private KeyspaceMetadata getLocalSystemKeyspaceMetadata(String name)
    {
        return localSystemKeyspaces.get(name);
    }

    KeyspaceMetadata getVirtualKeyspaceMetadata(String name)
    {
        VirtualKeyspace keyspace = virtualKeyspaces.get(name);
        return null != keyspace ? keyspace.metadata() : null;
    }

    public Iterable<KeyspaceMetadata> getVirtualKeyspacesMetadata()
    {
        return Iterables.transform(virtualKeyspaces.values(), VirtualKeyspace::metadata);
    }

    /**
     * The names of all the defined local system keyspaces.
     */
    Set<String> localSystemKeyspaceNames()
    {
        return localSystemKeyspaces.values()
                                   .stream()
                                   .map(k -> k.name)
                                   .collect(Collectors.toSet());
    }

    /**
     * The total number of defined local system tables.
     */
    int numberOfLocalSystemTables()
    {
        return localSystemKeyspaces.values().stream().mapToInt(k -> Iterables.size(k.tablesAndViews())).sum();
    }

    private void load(KeyspaceMetadata localKeyspace)
    {
        localSystemKeyspaces.put(localKeyspace.name, localKeyspace);
        for (TableMetadata tableMetadata : localKeyspace.tablesAndViews())
            localSystemTables.put(tableMetadata.id, tableMetadata);

        schemaManager.loadNew(localKeyspace);
    }

    public void load(VirtualKeyspace keyspace)
    {
        virtualKeyspaces.put(keyspace.name(), keyspace);
        keyspace.tables().forEach(t -> virtualTables.put(t.metadata().id, t));
    }
}
