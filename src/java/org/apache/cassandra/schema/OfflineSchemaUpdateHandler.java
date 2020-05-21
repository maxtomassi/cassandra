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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Schema manager that assumes we are "offline", by which we mean "not a real node".
 * <p>
 * Mainly, this only keeps the schema in memory, not reading/saving anything from/to disk, and handles the schema purely
 * locally, not exchanging anything with other nodes.
 */
public class OfflineSchemaUpdateHandler extends SchemaUpdateHandler<OfflineSchema>
{
    /**
     * Whether or not "stores" ({@link Keyspace} and {@link ColumnFamilyStore} instances and their dependent) should
     * be created and updated. Creating such stores basically requires loading most of the yaml file, will create data
     * directories if they don't exist, etc...
     * Some tools want that, but some don't (see comment in {@link #defaultCreateAndUpdateStores()} too).
     */
    private final boolean createAndUpdateStores = defaultCreateAndUpdateStores();

    private static boolean defaultCreateAndUpdateStores()
    {
        // We don't "create and update stores" for "clients" because that's what code using "client mode" expects
        // (typically CQLSSTableWriter uses "client" instead of "tool" mode just for that). Same reason for why "tools"
        // do return true ("that what most tools expect").
        // Arguably, this isn't extra intuitive and our namings (tools vs clients) is wacky, so we should clear
        // that all some day but in the meantime...
        return !DatabaseDescriptor.isClientInitialized();
    }

    /**
     * Creates an offline schema handler.
     *
     * @param manager the manager of the handler.
     */
    OfflineSchemaUpdateHandler(SchemaManager manager)
    {
        super("OFFLINE", manager);
    }

    @Override
    protected OfflineSchema emptySchema()
    {
        return OfflineSchema.EMPTY;
    }

    @Override
    CompletableFuture<Void> initializeSchemaFromDisk()
    {
        // Some tool do read the schema from disk so allow it.
        return CompletableFuture.runAsync(() -> {
            Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
            setInitialSchema(new OfflineSchema(keyspaces, 1));
        });
    }

    @Override
    CompletableFuture<SchemaTransformation.Result> apply(SchemaTransformation transformation,
                                                         boolean preserveExistingUserSettings)
    {
        return call(() -> {
            OfflineSchema before = currentSchema();
            Keyspaces afterKeyspaces = transformation.apply(before.keyspaces);
            KeyspacesDiff keyspacesDiff = KeyspacesDiff.diff(before.keyspaces, afterKeyspaces);
            // Don't bump the version if nothing has happened. Probably not necessary but more logical so ...
            long newVersion = keyspacesDiff.isEmpty() ? before.version : before.version + 1;
            OfflineSchema after = new OfflineSchema(afterKeyspaces, newVersion);
            return updateSchema(new SchemaTransformationResult<>(before, after, keyspacesDiff));
        });
    }

    @Override
    protected void applyChangesLocally(KeyspacesDiff diff)
    {
        if (!createAndUpdateStores)
            return;

        super.applyChangesLocally(diff);
    }

    @Override
    protected Keyspace openKeyspace(String keyspaceName)
    {
        // We're offline, don't load the sstables.
        return Keyspace.openWithoutSSTables(keyspaceName);
    }

    @Override
    protected void createStore(TableMetadata table)
    {
        // We're offline, don't load the sstables.
        openKeyspace(table.keyspace).initCf(manager.getTableMetadataRef(table.id), false);
    }

    @Override
    boolean isOnDiskSchemaKeyspace(String keyspaceName)
    {
        return false;
    }

    @Override
    void tryReloadingSchemaFromDisk()
    {
        throw new UnsupportedOperationException("Cannot reload schema from disk when offline");
    }

    @Override
    void waitUntilReadyForBootstrap()
    {
        throw new UnsupportedOperationException("We shouldn't be bootstrapping when offline");
    }

    @Override
    CompletableFuture<Void> onUpdatedSchemaVersion(InetAddressAndPort remote, UUID newSchemaVersionAsUUID, String reason)
    {
        throw new UnsupportedOperationException("We shouldn't get remote schema version when offline");
    }

    @Override
    void initializeGossipedSchemaInfo(Map<ApplicationState, VersionedValue> appStates)
    {
        throw new UnsupportedOperationException("We shouldn't be starting gossiper when offline");
    }

    @Override
    CompletableFuture<Void> ensureLocalNodeUpToDate()
    {
        // We're as up to date as we'll ever be.
        return CompletableFuture.completedFuture(null);
    }
}
