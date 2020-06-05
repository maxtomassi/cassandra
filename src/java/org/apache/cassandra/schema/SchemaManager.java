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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import static java.lang.String.format;

import static com.google.common.collect.Iterables.size;

public final class SchemaManager
{
    public static final SchemaManager instance = new SchemaManager();

    private final LocalKeyspaces localKeyspaces;

    private final SchemaUpdateHandler<?> updateHandler;

    // UUID -> mutable metadata ref map. We have to update these in place every time a table changes.
    private final Map<TableId, TableMetadataRef> metadataRefs = new NonBlockingHashMap<>();

    // (keyspace name, index name) -> mutable metadata ref map. We have to update these in place every time an index changes.
    private final Map<Pair<String, String>, TableMetadataRef> indexMetadataRefs = new NonBlockingHashMap<>();

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    private final Map<String, Keyspace> keyspaceInstances = new NonBlockingHashMap<>();

    final List<SchemaChangeListener> changeListeners = new CopyOnWriteArrayList<>();

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private SchemaManager()
    {
        this.updateHandler = DatabaseDescriptor.isDaemonInitialized()
                             ? new DefaultSchemaUpdateHandler(this)
                             : new OfflineSchemaUpdateHandler(this);

        this.localKeyspaces = new LocalKeyspaces(this);

        if (DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized())
        {
            this.localKeyspaces.loadHardCodedDefinitions();
        }
    }

    /**
     * The current schema.
     */
    public Schema schema()
    {
        return updateHandler.currentSchema();
    }

    /**
     * The current schema update handler.
     *
     * This is not meant to be exposed publicly, but is when some handler specific behavior goes through messaging to
     * obtain the current handler on the receiving side.
     */
    SchemaUpdateHandler<?> updateHandler()
    {
        return updateHandler;
    }

    public LocalKeyspaces localKeyspaces()
    {
        return localKeyspaces;
    }

    /**
     * Reads the version of the schema saved locally on disk and "loads" it.
     *
     * <p>This should only be called if the current schema is empty (and only once), and it may throw otherwise.
     */
    public void loadFromDisk()
    {
        SchemaDiagnostics.schemataLoading(this);
        updateHandler.initializeSchemaFromDisk();
        SchemaDiagnostics.schemataLoaded(this);
    }

    /**
     * For tests, clear the in-memory representation of the schema without going through a proper schema change, and
     * without updating the on-disk representation in particular.
     *
     * <p>This is used for testing reloading the schema from disk and should not be used otherwise as this is
     * completely unsafe.
     */
    @VisibleForTesting
    void clearInMemoryUnsafe()
    {
        updateHandler.clearSchemaUnsafe();
        SchemaDiagnostics.schemataCleared(this);
    }

    /**
     * Adds the {@link TableMetadataRef} corresponding to the provided keyspace to {@link #metadataRefs} and
     * {@link #indexMetadataRefs}, assuming the keyspace is new (in the sense of not being tracked by the manager yet).
     */
    void addNewRefs(KeyspaceMetadata ksm)
    {
        ksm.tablesAndViews()
           .forEach(metadata -> metadataRefs.put(metadata.id, new TableMetadataRef(metadata)));

        ksm.tables
        .indexTables()
        .forEach((name, metadata) -> indexMetadataRefs.put(Pair.create(ksm.name, name), new TableMetadataRef(metadata)));

        SchemaDiagnostics.metadataInitialized(this, ksm);
    }

    /**
     * Updates the {@link TableMetadataRef} in {@link #metadataRefs} and {@link #indexMetadataRefs}, for an
     * existing updated keyspace given it's previous and new definition.
     */
    void updateRefs(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Keyspace keyspace = getKeyspaceInstance(updated.name);
        if (null != keyspace)
            keyspace.setMetadata(updated);

        Tables.TablesDiff tablesDiff = Tables.diff(previous.tables, updated.tables);
        Views.ViewsDiff viewsDiff = Views.diff(previous.views, updated.views);

        MapDifference<String, TableMetadata> indexesDiff = previous.tables.indexesDiff(updated.tables);

        // clean up after removed entries
        tablesDiff.dropped.forEach(table -> metadataRefs.remove(table.id));
        viewsDiff.dropped.forEach(view -> metadataRefs.remove(view.metadata.id));

        indexesDiff.entriesOnlyOnLeft()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.remove(Pair.create(indexTable.keyspace, indexTable.indexName().get())));

        // load up new entries
        tablesDiff.created.forEach(table -> metadataRefs.put(table.id, new TableMetadataRef(table)));
        viewsDiff.created.forEach(view -> metadataRefs.put(view.metadata.id, new TableMetadataRef(view.metadata)));

        indexesDiff.entriesOnlyOnRight()
                   .values()
                   .forEach(indexTable -> indexMetadataRefs.put(Pair.create(indexTable.keyspace, indexTable.indexName().get()), new TableMetadataRef(indexTable)));

        // refresh refs to updated ones
        tablesDiff.altered.forEach(diff -> metadataRefs.get(diff.after.id).set(diff.after));
        viewsDiff.altered.forEach(diff -> metadataRefs.get(diff.after.metadata.id).set(diff.after.metadata));

        indexesDiff.entriesDiffering()
                   .values()
                   .stream()
                   .map(MapDifference.ValueDifference::rightValue)
                   .forEach(indexTable -> indexMetadataRefs.get(Pair.create(indexTable.keyspace, indexTable.indexName().get())).set(indexTable));

        SchemaDiagnostics.metadataReloaded(this, previous, updated, tablesDiff, viewsDiff, indexesDiff);
    }


    /**
     * Removes the {@link TableMetadataRef} from {@link #metadataRefs} and {@link #indexMetadataRefs} for the provided
     * (dropped) keyspace.
     */
    void removeRefs(KeyspaceMetadata ksm)
    {
        ksm.tablesAndViews()
           .forEach(t -> metadataRefs.remove(t.id));

        ksm.tables
        .indexTables()
        .keySet()
        .forEach(name -> indexMetadataRefs.remove(Pair.create(ksm.name, name)));

        SchemaDiagnostics.metadataRemoved(this, ksm);
    }

    public void registerListener(SchemaChangeListener listener)
    {
        changeListeners.add(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        changeListeners.remove(listener);
    }

    /**
     * Get keyspace instance by name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return Keyspace object or null if keyspace was not found
     */
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.get(keyspaceName);
    }

    public ColumnFamilyStore getColumnFamilyStoreInstance(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace instance = getKeyspaceInstance(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
             ? instance.getColumnFamilyStore(metadata.id)
             : null;
    }

    /**
     * Store given Keyspace instance to the schema
     *
     * @param keyspace The Keyspace instance to store
     *
     * @throws IllegalArgumentException if Keyspace is already stored
     */
    public void storeKeyspaceInstance(Keyspace keyspace)
    {
        if (keyspaceInstances.containsKey(keyspace.getName()))
            throw new IllegalArgumentException(String.format("Keyspace %s was already initialized.", keyspace.getName()));

        keyspaceInstances.put(keyspace.getName(), keyspace);
    }

    /**
     * Remove keyspace from schema
     *
     * @param keyspaceName The name of the keyspace to remove
     *
     * @return removed keyspace instance or null if it wasn't found
     */
    public Keyspace removeKeyspaceInstance(String keyspaceName)
    {
        return keyspaceInstances.remove(keyspaceName);
    }

    /**
     * The total number of tables defined, _including_ system ones (but excluding system views).
     */
    public int getNumberOfTables()
    {
        int numOfNonLocalTables = schema().keyspaces.stream().mapToInt(k -> size(k.tablesAndViews())).sum();
        return localKeyspaces.numberOfLocalSystemTables() + numOfNonLocalTables;
    }

    /**
     * Gets a materialized view metadata by name.
     *
     * @param keyspaceName the keyspace of the view.
     * @param viewName the name of the view.
     * @return the metadata for the view, or {@code null} if that view does not exists.
     */
    @Nullable
    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = schema().keyspaces.getNullable(keyspaceName);
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or {@code null} if it wasn't found
     */
    @Nullable
    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata keyspace = schema().keyspaces.getNullable(keyspaceName);

        if (keyspace != null)
            return keyspace;

        // Check if it's a local keyspace, or a virtual one.
        return localKeyspaces.getKeyspaceMetadata(keyspaceName);    }

    /**
     * The names of all non-local-system keyspaces.
     *
     * @return collection of the non-system keyspaces (note that this count as system only the
     * non replicated keyspaces, so keyspace like system_traces which are replicated are actually
     * returned. See getUserKeyspace() below if you don't want those)
     */
    public Set<String> getNonLocalSystemKeyspaces()
    {
        return schema().keyspaces.names();
    }

    /**
     * The names of all (non-virtual) replicated keyspaces, that is any keyspace (system distributed ones include) that
     * do not use {@link LocalStrategy}.
     */
    public List<String> getNonLocalStrategyKeyspaces()
    {
        return schema().keyspaces.stream()
                                 .filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class)
                                 .map(keyspace -> keyspace.name)
                                 .collect(Collectors.toList());
    }

    /**
     * All user created keyspaces.
     */
    public Set<String> getUserKeyspaces()
    {
        return Sets.filter(schema().keyspaces.names(), k -> !SchemaConstants.isInternalKeyspace(k));
    }

    /**
     * All the non-virtual keyspace names registered in the system (system and non-system).
     */
    public Set<String> getAllKeyspaces()
    {
        return Sets.union(schema().keyspaces.names(), localKeyspaces.localSystemKeyspaceNames());
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace, which must exists
     *
     * @return metadata about all the tables and views belonging to {@code keyspaceName}
     */
    public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        assert keyspaceName != null;
        KeyspaceMetadata ksm = getKeyspaceMetadata(keyspaceName);
        assert ksm != null;
        return ksm.tablesAndViews();
    }

    /* TableMetadata/Ref query/control methods */

    /**
     * Retrieves a table (or view) metadata reference by name.
     *
     * @param keyspaceName the name of the keyspace for the table metadata reference to get.
     * @param tableName the name of the table for which to retrieve a metadata reference.
     * @return the metadata reference for table {@code keyspaceName.tableName}, or {@code null} if it doesn't exists.
     */
    @Nullable
    public TableMetadataRef getTableMetadataRef(String keyspaceName, String tableName)
    {
        TableMetadata tm = getTableMetadata(keyspaceName, tableName);
        return tm == null ? null : getTableMetadataRef(tm.id);
    }

    /**
     * Retrieves a table (or view) metadata reference for an index by name.
     *
     * @param keyspaceName the name of the keyspace for the index whose table metadata reference to get.
     * @param indexName the name of the index for which to retrieve a metadata reference.
     * @return the metadata reference for index {@code keyspaceName.indexName} or {@code null} if it doesn't exists.
     */
    @Nullable
    public TableMetadataRef getIndexTableMetadataRef(String keyspaceName, String indexName)
    {
        return indexMetadataRefs.get(Pair.create(keyspaceName, indexName));
    }

    Map<Pair<String, String>, TableMetadataRef> getIndexTableMetadataRefs()
    {
        return indexMetadataRefs;
    }

    /**
     * Retrieves a table (or view) metadata reference by table identifier.
     *
     * @param id the table identifier for the table whose metadata reference to get.
     * @return the metadata reference for table {@code id}, or {@code null} if it doesn't exists.
     */
    @Nullable
    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        return metadataRefs.get(id);
    }

    Map<TableId, TableMetadataRef> getTableMetadataRefs()
    {
        return metadataRefs;
    }

    /**
     * Retrieves a table (or view, and including virtual tables) metadata by name.
     *
     * @param keyspace the keyspace name, which must not be {@code null}.
     * @param table the table name, which must not be {@code null}.
     * @return the metadata for {@code keyspace.table}, or {@code null} if it doesn't exists.
     */
    @Nullable
    public TableMetadata getTableMetadata(String keyspace, String table)
    {
        assert keyspace != null;
        assert table != null;

        KeyspaceMetadata ksm = getKeyspaceMetadata(keyspace);
        return ksm == null
             ? null
             : ksm.getTableOrViewNullable(table);
    }

    /**
     * Retrieves a table (or view, and including virtual tables) metadata by ID.
     *
     * @param id the ID of the table to retriever.
     * @return the metadata for table {@code id}, or {@code null} if it doesn't exists.
     */
    @Nullable
    public TableMetadata getTableMetadata(TableId id)
    {
        TableMetadata table = schema().keyspaces.getTableOrViewNullable(id);
        return table == null ? localKeyspaces.getTableMetadata(id) : table;
    }

    /**
     * Validates that a table (or view, and including virtual tables) exists, returning its metadata if it does and
     * throwing otherwise.
     *
     * @param keyspaceName the keyspace name, which must not be {@code null}.
     * @param tableName the table name, which must not be {@code null}.
     * @return the metadata for existing table {@code keyspaceName.tableName}.
     *
     * @throws KeyspaceNotDefinedException if {@code keyspaceName} does not exist.
     * @throws InvalidRequestException if {@code tableName} does not exist within {@code keyspaceName}.
     */
    public TableMetadata validateTableForUserQuery(String keyspaceName, String tableName)
    {
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        KeyspaceMetadata keyspace = getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException(format("keyspace %s does not exist", keyspaceName));

        TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
        if (metadata == null)
            throw new InvalidRequestException(format("table %s does not exist", tableName));

        return metadata;
    }

    /**
     * Retrieves a table (or view, and including virtual tables) metadata by ID, throwing if such table does not exist.
     *
     * @throws UnknownTableException if the table couldn't be found in the metadata.
     */
    public TableMetadata getExistingTableMetadata(TableId id) throws UnknownTableException
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata != null)
            return metadata;

        String message =
            String.format("Couldn't find table with id %s. If a table was just created, this is likely due to the schema"
                          + "not being fully propagated.  Please wait for schema agreement on table creation.",
                          id);
        throw new UnknownTableException(message, id);
    }

    /* Function helpers */

    /**
     * Get all function overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the keyspace or the function name are not found;
     *         a non-empty collection of {@link Function} otherwise
     */
    public Collection<Function> getFunctions(FunctionName name)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
             ? Collections.emptyList()
             : ksm.functions.get(name);
    }

    /**
     * Find the function with the specified name
     *
     * @param name fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    public Optional<Function> findFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
             ? Optional.empty()
             : ksm.functions.find(name, argTypes);
    }

    /* Version control */

    /**
     * The current schema version in UUID form.
     *
     * @return the current schema UUID version.
     */
    public UUID getVersionAsUUID()
    {
        return schema().versionAsUUID();
    }

    /**
     * Checks whether the given schema version is the same as the current local schema.
     */
    public boolean isSameVersion(UUID schemaVersion)
    {
        return schemaVersion != null && schemaVersion.equals(getVersionAsUUID());
    }

    /**
     * Checks whether the current schema is empty, which is defined by having no keyspace defined <em>other than</em>
     * the hard-coded <em>local</em> system ones. Note in particular that the schema is not considered empty as soon as
     * non-local system keyspace are created, so an empty schema correspond to a brand new node that has not received
     * any schema yet nor finished joining the ring (since distributed system keyspace are created then if not before).
     */
    public boolean isEmpty()
    {
        return schema().isEmpty();
    }

    /**
     * Whether the provided keyspace is the system keyspace used by this updater to store the local version of the
     * schema (that is, the keyspace read by {@link #loadFromDisk()}) and {@link #tryReloadingSchemaFromDisk()}.
     *
     * @param keyspaceName the keyspace to check.
     * @return {@code true} if {@code keyspace} is a schema system keyspace (for the active schema management
     * handler).
     */
    public boolean isOnDiskSchemaKeyspace(String keyspaceName)
    {
        return updateHandler.isOnDiskSchemaKeyspace(keyspaceName);
    }

    /**
     * Reads the schema stored locally on disk, and replace the local view of the schema with that.
     *
     * @return a future on the completion of the reloading the schema from disk and applying any necessary changes.
     */
    public void tryReloadingSchemaFromDisk()
    {
        updateHandler.tryReloadingSchemaFromDisk();
    }

    /**
     * Apply the provided schema transformation to the current schema (if said transformation is valid).
     *
     * @param transformation the transformation to applySchemaMigration to the current schema.
     * @return a future on the completion of the update. If the application is valid and successful (the schema has
     * been applied, _at least_ locally), the future will complete with the result of the change made. Otherwise, the
     * future will complete exceptionally.
     */
    public SchemaTransformation.Result apply(SchemaTransformation transformation)
    {
        return updateHandler.apply(transformation);
    }

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
     * and later evolved on major upgrades (sometimes minor too). This method applies the necessary transformation to
     * the current known definition of the keyspace in order to get to the given expected definition.
     *
     * NB: this method should only be called for changes to internal keyspaces and NEVER be called for user keyspaces.
     *
     * @param keyspace the expected modern definition of the keyspace
     * @param generation the generation of the given keyspace definition. When a keyspace definition gets updated the
     *                   generation number should be bumped accordingly. The application of the given transformation
     *                   should not override changes that could be potentially have been made by the user at keyspace
     *                   and tables level, typically update the replication factor for a keyspace.
     *
     * @return the result of the transformation that has been applied. Calling {@link SchemaTransformation.Result#isEmpty()}
     *         on the result will return true if no change has been made (the given keyspace is up to date).
     *
     */
    public SchemaTransformation.Result evolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        // This should not be used for a user keyspace ever.
        assert SchemaConstants.isInternalKeyspace(keyspace.name) : keyspace.name + " is not an internal keyspace";

        return updateHandler.apply(SchemaTransformations.evolveSystemKeyspace(keyspace), generation);
    }

    /**
     * Equivalent to {@link #evolveSystemKeyspace(KeyspaceMetadata, long)} but it can also be called for
     * a non-system keyspace.
     *
     * NB: This should only be used for testing purposes as might cause issues in production code if called on
     *     user keyspaces.
     */
    @VisibleForTesting
    SchemaTransformation.Result forceEvolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        return updateHandler.apply(SchemaTransformations.evolveSystemKeyspace(keyspace), generation);
    }

    /**
     * If legacy schema handling is used, clears all locally stored schema information, reset schema to initial state,
     * and force the local schema to that of another live node if any is available.
     * <p>
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     *
     * @throws IllegalStateException if legacy schema handling is not in use.
     */
    public void resetLegacyLocalSchema()
    {
        SchemaUpdateHandler handler = updateHandler; // making sure we don't race with a change of handler
        if (!(handler instanceof DefaultSchemaUpdateHandler))
            throw new IllegalStateException(format("Resetting the local schema is not allowed with %s schema handling",
                                                   handler.name()));

        ((DefaultSchemaUpdateHandler) handler).resetLocalSchema();
    }

    /**
     * Method called back when we learn about a remote having a new schema version (or at least we suspect it may
     * have; calling this method "uselessly" is inefficient (so don't overdo it) but otherwise harmless).
     *
     * @param remote the remote node having had a schema change.
     * @param newSchemaVersionAsUUID the new version (as a UUID) of the remote.
     * @param releaseVersion the releaseVersion of the remote endpoint.
     */
    public void onUpdatedSchemaVersion(InetAddressAndPort remote,
                                       UUID newSchemaVersionAsUUID,
                                       String releaseVersion)
    {
        updateHandler.onUpdatedSchemaVersion(remote, newSchemaVersionAsUUID, releaseVersion);
    }

    /**
     * Assuming this node is bootstrapping, tests whether the schema on this node is in sufficiently up-to-date state
     * to move along with the bootstrap.
     */
    public boolean isReadyForBootstrap()
    {
        // if there is only live node in the cluster, it is ready for bootstrap
        if (Gossiper.instance.getLiveMembers().size() == 1)
            return true;

        // this node's schema should match schema of at least one other node in the cluster
        for (InetAddressAndPort endpoint : Gossiper.instance.getEndpoints())
        {
            if (!endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) && isSameVersion(Gossiper.instance.getSchemaVersion(endpoint)))
                return true;
        }
        return false;
    }

    /**
     * Assuming this node is bootstrapping, block until the schema on this node is in sufficiently up-to-date state
     * to move along with bootstrap, that is until {@link #isReadyForBootstrap()} returns {@code true}.
     */
    public void waitUntilReadyForBootstrap()
    {
        updateHandler.waitUntilReadyForBootstrap();
    }

    /**
     * Called on startup, before starting {@link Gossiper}, to allow the schema to populate the few
     * {@link ApplicationState} it uses.
     *
     * @param appStates the initial {@link ApplicationState}
     */
    public void addSchemaStaticInfoForGossiper(Map<ApplicationState, VersionedValue> appStates)
    {
        updateHandler.initializeGossipedSchemaInfo(appStates);
    }

    /**
     * Slight abstraction leak, we need to reference {@link MigrationManager} from {@link SchemaPushVerbHandler}
     *
     * @return the {@link MigrationManager} if legacy schema code is in use, {@code null} otherwise.
     */
    public MigrationManager legacyMigrationManager()
    {
        SchemaUpdateHandler<?> handler = updateHandler;
        return handler instanceof DefaultSchemaUpdateHandler
               ? ((DefaultSchemaUpdateHandler) handler).migrationManager
               : null;

    }
}
