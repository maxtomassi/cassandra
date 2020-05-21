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

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.TriConsumer;

/**
 * Abstract the handling of schema updates (and synchronization between nodes).
 * <p>
 * This exists mainly because we support 2 modes of schema updates: the legacy non-concurrent one, and the concurrent
 * mode (introduced by DB-767), though is also currently used to handle tools more cleanly (through
 * {@link OfflineSchemaUpdateHandler}; currently, some tools have to rely on using {@link SchemaManager#instance}
 * directly; that's obviously not ideal and we should refactor the code so that tools can, say, use a special version
 * of {@link SchemaManager} directly (rather than using the one singleton and having that internally use a special
 * handler based on whether {@link DatabaseDescriptor#toolInitialization()} was called or not). If/When we get rid of
 * the legacy mode and have refactor the singleton use, this can likely be folded into {@link SchemaManager} more
 * directly.
 * <p>
 * Note that the handler basically simply manages the current {@link Schema}, and in particular is not in charge of
 * local system table or virtual ones (see {@link LocalKeyspaces} for that).
 */
abstract class SchemaUpdateHandler<S extends Schema>
{
    private static final Stage EXECUTOR_STAGE = Stage.SCHEMA_UPDATES;

    private final String name;

    /**
     * Single thread executor on which all changes to {@link #currentSchema} are performed, basically serializing
     * changes to the local schema.
     */
    private final Executor executor = EXECUTOR_STAGE.executor();

    /**
     * Executor used for any potentially blocking actions resulting for a schema update. See
     * {@link #applyChangesLocally(KeyspacesDiff)} for more information on when this is used.
     */
    private final Executor updateCompletionExecutorupdateCompletionExecutor = TPC.ioScheduler().forTaskType(TPCTaskType.SCHEMA_UPDATE_COMPLETION);

    protected final SchemaManager manager;

    private volatile S currentSchema;

    SchemaUpdateHandler(String name, SchemaManager manager)
    {
        this.name = name;
        this.manager = manager;
        this.currentSchema = emptySchema();
    }

    /**
     * The name of this handler, that is a string describing the handler for error/debug/info messages.
     */
    String name()
    {
        return name;
    }

    /**
     * The empty schema for the {@link Schema} implementation used by this update handler
     */
    protected abstract S emptySchema();

    /**
     * The current schema.
     */
    S currentSchema()
    {
        return currentSchema;
    }

    /**
     * Ensures the provided runnable on {@link #executor} but:
     * - with "re-entrancy": if the current thread is already on {@link #executor}, the runnable is executed directly
     *   (so that waiting on the future in that case does not deadlock). Note that this _does_ mean that tasks running
     *   on {@link #executor} may intermingle a bit, but this is necessary in a few cases (the main one being the
     *   case of custom indexes: the creation of indexes happens as part {@link #applyChangesLocally}, but some
     *   custom indexes can theoretically trigger (and wait on) schema changes themselves, and we have at least the
     *   test class {@code TableBackedCustomIndex} that does that at the time of this writing).
     * - it ensures the returned future is _not_ completed on {@link #executor}. That is, while the task itself will
     *   execute on said {@link #executor}, we switch to {@link #updateCompletionExecutor} () just for the sake
     *   of the final completion of the future. The reason being that as {@link #executor} is single threaded and
     *   critical to schema execution, we want to control what runs into it, and so we want to avoid having tasks
     *   chained on the returned future to execute on it. In particular, some DSE code "chain" tasks that block and
     *   while we could fix those, it is likely safer this way.
     */
    protected CompletableFuture<Void> run(Runnable runnable)
    {
        return execute(s -> {
            try
            {
                runnable.run();
                s.complete(null);
            }
            catch (Throwable t)
            {
                s.completeExceptionally(t);
            }
        });
    }

    protected <V> CompletableFuture<V> call(Supplier<V> supplier)
    {
        return execute(s -> {
            try
            {
                s.complete(supplier.get());
            }
            catch (Throwable t)
            {
                s.completeExceptionally(t);
            }
        });
    }

    protected interface CompletionSignaler<V>
    {
        void complete(V value);
        void completeExceptionally(Throwable t);
    }

    protected <V> CompletableFuture<V> execute(Consumer<CompletionSignaler<V>> task)
    {
        CompletableFuture<V> future = new CompletableFuture<>();
        CompletionSignaler<V> signaler = new CompletionSignaler<>()
        {
            @Override
            public void complete(V value)
            {
                updateCompletionExecutor.execute(() -> future.complete(value));
            }

            @Override
            public void completeExceptionally(Throwable t)
            {
                updateCompletionExecutor.execute(() -> future.completeExceptionally(t));
            }
        };

        if (isOnExecutor())
            task.accept(signaler);
        else
            executor.execute(() -> task.accept(signaler));
        return future;
    }

    protected boolean isOnExecutor() {
        return EXECUTOR_STAGE.equals(Stages.currentThreadStage());
    }

    /**
     * Reset the current schema to the empty one.
     * <p>
     * Note: we don't require it to be called on the executor because, well, unsafe is in the name.
     */
    protected void clearSchemaUnsafe()
    {
        currentSchema = emptySchema();
    }

    /**
     * Load the initial schema (called on startup), setting it as the current schema (this is somewhat similar
     * to {@link #updateSchema} but does not call {@link #applyChangesLocally} or {@link #notifyChanges}).
     * <p>
     * When called, the current schema must be empty.
     * <p>
     * Must be called on the handler executor, that is inside a call to {@link #run} or {@link #call}.
     *
     * @param initialSchema the initial schema to set.
     */
    protected void setInitialSchema(S initialSchema)
    {
        assert isOnExecutor();
        assert currentSchema.isEmpty() : "Current schema: " + currentSchema;
        currentSchema = initialSchema;
        currentSchema.keyspaces().forEach(manager::addNewRefs);
    }

    /**
     * Update the current schema definition and perform any change necessary on the local node for the update (through
     * {@link #applyChangesLocally} and {@link #notifyChanges}).
     * <p>
     * Must be called on the handler executor, that is inside a call to {@link #run} or {@link #call}.
     *
     * @param update the result of the update made to the schema and to applySchemaMigration. The
     * {@link SchemaTransformationResult#before} value must still be the current active schema.
     */
    protected <R extends SchemaTransformationResult<S>> R updateSchema(R update)
    {
        assert isOnExecutor();
        assert currentSchema == update.before;
        if (update.isEmpty())
            return update;

        currentSchema = update.after;

        KeyspacesDiff diff = update.diff();
        updateRefs(diff);
        applyChangesLocally(diff);
        notifyChanges(diff);
        return update;
    }

    /**
     * Update/create/drop the {@link TableMetadataRef} in {@link SchemaManager}.
     */
    private void updateRefs(KeyspacesDiff diff)
    {
        diff.dropped.forEach(manager::removeRefs);
        diff.created.forEach(manager::addNewRefs);
        diff.altered.forEach(delta -> manager.updateRefs(delta.before, delta.after));
    }

    /**
     * Reload/create/drop {@link Keyspace} and {@link ColumnFamilyStore} instances.
     */
    protected void applyChangesLocally(KeyspacesDiff diff)
    {
        assert isOnExecutor(); // checking because it's protected, not private, but it's protected so it can be
        // overriden so hopefully not an issue..
        diff.dropped.forEach(this::dropKeyspace);
        diff.created.forEach(this::createKeyspace);
        diff.altered.forEach(this::alterKeyspace);
    }

    // TODO: [max] this is done
    private void dropKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceDroping(manager, keyspace);

        keyspace.views.forEach(v -> dropView(keyspace, v));
        keyspace.tables.forEach(t -> dropTable(keyspace, t));

        // remove the keyspace from the static instances, and make sure no one can try to recreate it while we
        // are removing the metadata
        Keyspace instance = manager.removeKeyspaceInstance(keyspace.name);
        assert instance != null;

        Keyspace.clear(keyspace.name);

        Keyspace.writeOrder.awaitNewBarrier();
        SchemaDiagnostics.keyspaceDroped(manager, keyspace);
    }

    // TODO: [max] this is done
    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(manager, keyspace);
        openKeyspace(keyspace.name);
        SchemaDiagnostics.keyspaceCreated(manager, keyspace);
    }

    // TODO: [max] this is done
    private void alterKeyspace(KeyspaceMetadata.Diff delta)
    {
        SchemaDiagnostics.keyspaceAltering(manager, delta);

        // drop tables and views
        delta.views.dropped.forEach(v -> dropView(delta.before, v));
        delta.tables.dropped.forEach(t -> dropTable(delta.before, t));

        // add tables and views
        delta.tables.created.forEach(this::createTable);
        delta.views.created.forEach(this::createView);

        // update tables and views
        delta.tables.altered.forEach(diff -> alterTable(diff.after));
        delta.views.altered.forEach(diff -> alterView(diff.after));

        // deal with all added and altered views
        openKeyspace(delta.after.name).viewManager.reload(true);

        SchemaDiagnostics.keyspaceAltered(manager, delta);
    }

    // TODO: [max] this is done
    private void dropView(KeyspaceMetadata keyspaceBefore, ViewMetadata view)
    {
        // Please note that at this point, openKeyspace(metadata.keyspace) is the "after" keyspace, but onTableDropped
        // that is called in dropTable needs the "before" instance (see the onTableDropped() javadoc for details).
        openKeyspace(view.metadata.keyspace).viewManager.dropView(view.metadata.name);
        dropTable(keyspaceBefore, view.metadata);
    }

    // TODO: [max] this is done
    private void dropTable(KeyspaceMetadata keyspaceBefore, TableMetadata metadata)
    {
        SchemaDiagnostics.tableDropping(manager, metadata);

        ColumnFamilyStore cfs = openKeyspace(metadata.keyspace).getColumnFamilyStore(metadata.id);
        assert cfs != null;
        cfs.onTableDropped(keyspaceBefore);
        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));
        openKeyspace(metadata.keyspace).dropCf(metadata.id);

        SchemaDiagnostics.tableDropped(manager, metadata);
    }

    // TODO: [max] this is done
    private void createTable(TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(manager, table);
        createStore(table);
        SchemaDiagnostics.tableCreated(manager, table);
    }

    // TODO: [max] this is done
    // it's a single line method but keeping it for symmetry with createTable
    private void createView(ViewMetadata view)
    {
        createStore(view);
    }

    // TODO: [max] this is done
    private void alterTable(TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(manager, updated);
        reloadStore(updated);
        SchemaDiagnostics.tableAltered(manager, updated);
    }

    // TODO: [max] this is done
    // it's a single line method but keeping it for symmetry with alterTable
    private void alterView(ViewMetadata updated)
    {
        reloadStore(updated);
    }

    /**
     * Called when retrieving a {@link Keyspace} instead, so equivalent to calling {@link Keyspace#open(String)} but
     * abstracted away for override by {@link OfflineSchemaUpdateHandler}.
     */
    protected Keyspace openKeyspace(String keyspaceName)
    {
        return Keyspace.open(keyspaceName);
    }

    /**
     * Called when a new table is created to initialize its {@link ColumnFamilyStore}. Protected for override by
     * {@link OfflineSchemaUpdateHandler}.
     */
    protected void createStore(TableMetadata table)
    {
        openKeyspace(table.keyspace).initCf(manager.getTableMetadataRef(table.id), true);
    }

    /**
     * Called when a new view is created to initialize its {@link ColumnFamilyStore}. Protected for override by
     * {@link OfflineSchemaUpdateHandler}.
     */
    protected void createStore(ViewMetadata view)
    {
        openKeyspace(view.metadata.keyspace).initCf(manager.getTableMetadataRef(view.metadata.id), true);
    }

    private void reloadStore(TableMetadata updated)
    {
        openKeyspace(updated.keyspace).getColumnFamilyStore(updated.name).reload();
    }

    private void reloadStore(ViewMetadata updated)
    {
        openKeyspace(updated.metadata.keyspace).getColumnFamilyStore(updated.metadata.name).reload();
    }

    /**
     * Notify the {@link SchemaChangeListener} registered in {@link SchemaManager}.
     */
    private void notifyChanges(KeyspacesDiff diff)
    {
        diff.dropped.forEach(this::notifyDroppedKeyspace);
        diff.created.forEach(this::notifyCreatedKeyspace);
        diff.altered.forEach(this::notifyAlteredKeyspace);
    }

    private void notifyDroppedKeyspace(KeyspaceMetadata keyspace)
    {
        notify(() -> keyspace.functions.udas().iterator(), SchemaChangeListener::onDropAggregate);
        notify(() -> keyspace.functions.udfs().iterator(), SchemaChangeListener::onDropFunction);
        notify(keyspace.views, SchemaChangeListener::onDropView);
        notify(keyspace.tables, SchemaChangeListener::onDropTable);
        notify(keyspace.types, SchemaChangeListener::onDropType);
        notify(Collections.singleton(keyspace), SchemaChangeListener::onDropKeyspace);
    }

    private void notifyCreatedKeyspace(KeyspaceMetadata keyspace)
    {
        notify(Collections.singleton(keyspace), SchemaChangeListener::onCreateKeyspace);
        notify(keyspace.types, SchemaChangeListener::onCreateType);
        notify(keyspace.tables, SchemaChangeListener::onCreateTable);
        notify(keyspace.views, SchemaChangeListener::onCreateView);
        notify(() -> keyspace.functions.udfs().iterator(), SchemaChangeListener::onCreateFunction);
        notify(() -> keyspace.functions.udas().iterator(), SchemaChangeListener::onCreateAggregate);
    }

    private void notifyAlteredKeyspace(KeyspaceMetadata.Diff delta)
    {
        // notify on everything dropped
        notify(delta.udas.dropped, (l, f) -> l.onDropAggregate((UDAggregate) f));
        notify(delta.udfs.dropped, (l, f) -> l.onDropFunction((UDFunction)f));
        notify(delta.views.dropped, SchemaChangeListener::onDropView);
        notify(delta.tables.dropped, SchemaChangeListener::onDropTable);
        notify(delta.types.dropped, SchemaChangeListener::onDropType);

        // notify on everything created
        notify(delta.types.created, SchemaChangeListener::onCreateType);
        notify(delta.tables.created, SchemaChangeListener::onCreateTable);
        notify(delta.views.created, SchemaChangeListener::onCreateView);
        notify(delta.udfs.created, (l, f) -> l.onCreateFunction((UDFunction)f));
        notify(delta.udas.created, (l, f) -> l.onCreateAggregate((UDAggregate) f));

        // notify on everything altered
        if (!delta.before.params.equals(delta.after.params))
            notifyAlter(Collections.singleton(delta), SchemaChangeListener::onAlterKeyspace);
        notifyAlter(delta.types.altered, SchemaChangeListener::onAlterType);
        notifyAlter(delta.tables.altered, SchemaChangeListener::onAlterTable);
        notifyAlter(delta.views.altered, SchemaChangeListener::onAlterView);
        notifyAlter(delta.udfs.altered, SchemaChangeListener::onAlterFunction);
        notifyAlter(delta.udas.altered, SchemaChangeListener::onAlterAggregate);
    }

    private <O> void notify(Iterable<O> created, BiConsumer<SchemaChangeListener, O> notification)
    {
        created.forEach(o -> manager.changeListeners.forEach(l -> notification.accept(l, o)));
    }

    private <O> void notifyAlter(Iterable<Diff.Altered<O>> altered, TriConsumer<SchemaChangeListener, O, O> notification)
    {
        altered.forEach(a -> manager.changeListeners.forEach(l -> notification.accept(l, a.before, a.after)));
    }

    /**
     * Announce to "external stakeholder" that a new version of the schema has been applied (both other nodes through
     * Gossiper and drivers/users through the {@link SystemKeyspace#LOCAL} table).
     */
    protected void announceVersionUpdate(UUID newVersion)
    {
        // Always update the local system table. Additionally, advertise the change in gossip if we're in daemon mode
        SystemKeyspace.updateSchemaVersion(newVersion);
        SchemaDiagnostics.versionUpdated(manager);

        if (Gossiper.instance.isEnabled())
        {
            Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA,
                                                       StorageService.instance.valueFactory.schema(newVersion));
        }
    }

    /**
     * Reads the version of the schema saved locally on disk and "loads" it.
     *
     * <p>This should only be called if the current schema is empty (and only once), and it may throw otherwise.
     *
     * @return a future on the completion of initializing the schema from the local on-disk definitions.
     */
    abstract CompletableFuture<Void> initializeSchemaFromDisk();

    /**
     * Apply the provided transformation to the current schema.
     *
     * @param transformation the transformation to applySchemaMigration to the current schema.
     * @param preserveExistingUserSettings if true, any update  made by the transformation to an existing entity
     *        will not modify any prior settings set by users. The reason this exists is due to the handling of system
     *        distributed tables with the legacy schema code: for those, we have to create any that don't exists on
     *        startup, and even update those that do exists but have a modified structure (added column typically), but
     *        as users can update some of those tables settings (say, update the replication factor for the auth
     *        keyspace, or enable nodesync on a table) and we don't want to override those due to some internal update.
     *        So in that case, the legacy schema code uses a special timestamp of 0 (with the intent that this would be
     *        raised if we make internal updates that needs to override older versions, which hasn't yet happen) for
     *        it's schema table updates (in other words, anything made by a user, which will have a "normal" timestamp,
     *        will always take precedence on reconciliation and never be overridden). In any case, that's why we need
     *        this boolean and it should only be set to {@code true} in this very special case.
     * @return a future on the completion of the transformation. If the applied transformation is successful, said
     * future will complete normally with the result of the change. Otherwise, if the transformation is invalid or
     * cannot be applied for any other reason, the future will complete exceptionally.
     */
    abstract CompletableFuture<SchemaTransformation.Result> apply(SchemaTransformation transformation,
                                                                  boolean preserveExistingUserSettings);

    /**
     * See {@link SchemaManager#isOnDiskSchemaKeyspace(String)}.
     */
    abstract boolean isOnDiskSchemaKeyspace(String keyspaceName);

    /**
     * See {@link SchemaManager#tryReloadingSchemaFromDisk()}.
     */
    abstract void tryReloadingSchemaFromDisk();

    /**
     * Should only be called during bootstrap, when we need to ensure our schema is sufficiently up-to-date to continue
     * bootstrapping the node. What that means exactly and how long we may wait is implementation dependent.
     */
    abstract void waitUntilReadyForBootstrap();

    /**
     * Should be called when receiving a new schema version from a remote host (typically through Gossip).
     * This may trigger action to refresh the current schema if that remote version differs from our local one, though
     * any such work is done asynchronously from this method call.
     */
    abstract CompletableFuture<Void> onUpdatedSchemaVersion(InetAddressAndPort remote,
                                                            UUID newSchemaVersionAsUUID,
                                                            String reason);

    /**
     * Called before starting {@link Gossiper} so the handler can add any state that needs to be gossiped (typically,
     * at least the current schema version).
     *
     * @param appStates a map of the states to be passed to {@link Gossiper} when starting it. New states needed by this
     * handler should be added to this map.
     */
    void initializeGossipedSchemaInfo(Map<ApplicationState, VersionedValue> appStates)
    {
        appStates.put(ApplicationState.SCHEMA,
                      StorageService.instance.valueFactory.schema(currentSchema().versionAsUUID()));
    }

    /**
     * Ensure that the local node is up to date on any schema change that has been done in the cluster before this call.
     *
     * <p>Note that this method is non-blocking, so callers must wait on the returned future to guarantee the local
     * node is "up-to-date".
     *
     * @return a future that is completed when we know that any schema change made before this call (including schema
     * changes made by _other_ nodes) has been applied locally.
     */
    abstract CompletableFuture<Void> ensureLocalNodeUpToDate();
}