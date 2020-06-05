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

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Default schema handler, handling live (non-offline) schema updates.
 */
class DefaultSchemaUpdateHandler extends SchemaUpdateHandler<DefaultSchema>
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchemaUpdateHandler.class);

    final MigrationManager migrationManager;

    DefaultSchemaUpdateHandler(SchemaManager manager)
    {
        super("LEGACY", manager);
        this.migrationManager = new MigrationManager(this);
    }

    @Override
    protected DefaultSchema emptySchema()
    {
        return DefaultSchema.EMPTY;
    }

    @Override
    void initializeSchemaFromDisk()
    {
        run(() -> {
            // Note: it would be faster to compute the version while fetching the definitions, but since this is
            // the legacy schema handling, not bothering refactoring this.
            Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
            UUID version = SchemaKeyspace.calculateSchemaDigest();
            setInitialSchema(new DefaultSchema(keyspaces, version));
            if (!keyspaces.isEmpty())
                announceVersionUpdate(version);
        });
    }

    @Override
    SchemaTransformation.Result apply(SchemaTransformation transformation)
    {
        return apply(transformation, Optional.empty());
    }


    /**
     * See {@link SchemaUpdateHandler#apply(SchemaTransformation, long)}
     *
     * Some notes on this implementation:
     *
     * the {@param generation} number is the actual timestamp used for the schema changes mutations.
     * That's because the given keyspace might already exist but we want to modify its structure
     * (add a column typically). Since users can update some of those tables settings (say, update
     * the replication factor for the auth keyspace), we don't want to override those due to some internal update.
     *
     * Therefore, in that case, we use timestamp=0 for keyspace related mutations and
     * timestamp={@param generation} for table related mutations, so that any change made by a user,
     * which will have a "normal" timestamp, will always take precedence on reconciliation and
     * never be overridden.
     */
    @Override
    SchemaTransformation.Result apply(SchemaTransformation transformation, long generation)
    {
        return apply(transformation, Optional.of(generation));
    }

    SchemaTransformation.Result apply(SchemaTransformation transformation, Optional<Long> generation)
    {
        return call(() -> {
            // Check if the change applies and does something...
            DefaultSchema before = currentSchema();
            Keyspaces afterKeyspaces = transformation.apply(before.keyspaces);
            KeyspacesDiff keyspacesDiff = KeyspacesDiff.diff(before.keyspaces, afterKeyspaces);

            if (keyspacesDiff.isEmpty())
            {
                logger.debug("Applied no-op schema transformation {}", transformation);
                return new DefaultSchemaTransformationResult(before, before, keyspacesDiff, Collections.emptyList());
            }

            // ... it does, save those changes to the schema tables on disk
            Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(keyspacesDiff, generation);
            SchemaKeyspace.applyChanges(mutations);
            // Sets the new schema as current, applying any relevant changes in memory
            DefaultSchema after = new DefaultSchema(afterKeyspaces, SchemaKeyspace.calculateSchemaDigest());
            DefaultSchemaTransformationResult result = new DefaultSchemaTransformationResult(before,
                                                                                             after,
                                                                                             keyspacesDiff,
                                                                                             mutations);
            updateSchema(result);

            // Once we successfully applied the schema locally, we shouldn't fail the future as this would lead
            // consumers to assume the schema application has failed. So catch unexpected exception and log, but
            // don't rethrow.
            try
            {
                migrationManager.pushMigrationToOtherNodes(result.mutations());
                announceVersionUpdate(result.after.versionAsUUID());
            }
            catch (Exception e)
            {
                logger.error("Unexpected error announcing schema transformation {} to other nodes. This is a bug and "
                             + "should be reported to DataStax support. The schema update has however been "
                             + "successfully applied _locally_", transformation, e);
            }

            return result;
        });
    }

    @Override
    boolean isOnDiskSchemaKeyspace(String keyspaceName)
    {
        return SchemaConstants.isSchemaKeyspace(keyspaceName);
    }

    @Override
    void tryReloadingSchemaFromDisk()
    {
        Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
        apply(existing -> keyspaces);
    }

    /**
     * Apply a schema migration received from a remote node.
     *
     * @param mutations the mutations of the schema migration to apply
     * @return a future on the completion of the application of the migration.
     */
    void applySchemaMigration(Collection<Mutation> mutations)
    {
        run(() -> {
            DefaultSchema before = currentSchema();
            Keyspaces beforeKeyspaces = before.keyspaces;

            // Write the change to the schema tables first.
            SchemaKeyspace.applyChanges(mutations);

            Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(mutations);

            // Now fetch the schema resulting of this application for the keyspaces affected by the change, and build
            // the new schema based on those
            Keyspaces updatedKeyspaces = SchemaKeyspace.fetchKeyspaces(affectedKeyspaces);
            Keyspaces afterKeyspaces = beforeKeyspaces.withAddedOrReplaced(updatedKeyspaces);
            KeyspacesDiff keyspacesDiff = KeyspacesDiff.diff(beforeKeyspaces, afterKeyspaces);
            DefaultSchema after = new DefaultSchema(afterKeyspaces, SchemaKeyspace.calculateSchemaDigest());
            updateSchema(new DefaultSchemaTransformationResult(before, after, keyspacesDiff, mutations));
            announceVersionUpdate(after.versionAsUUID());
        });
    }

    // TODO: this method is kind of broken/dangerous because clearing the local schema is not safe at all. First,
    //   this method is presumably meant to be called when a node is online (otherwise, just hard-removing the system
    //   schema tables is probably easier/safer) but, even if we try to pull from another node right away, there will
    //   be a window during which the node has no schema and queries will likely fail while that is.
    //   But more importantly, this drops all the TableMetadataRef from SchemaManager, but existing instances of
    //   ColumnFamilyStore (and other consumers) will still refer to them. So even after the schema is restored from
    //   the schema PULL, those ColumnFamilyStore instance will refer to the old refs that will not get updated and
    //   that could lead to silent unexpected behavior while the node is not restarted.
    void resetLocalSchema()
    {
        logger.info("Starting local schema reset...");

        logger.debug("Truncating schema tables...");

        SchemaMigrationDiagnostics.resetLocalSchema();
        SchemaKeyspace.truncate();

        run(() -> {
            logger.debug("Clearing local schema keyspace definitions...");
            currentSchema().keyspaces.forEach(manager::removeRefs);
            clearSchemaUnsafe();
            logger.debug("Pulling schema from another node...");
            FBUtilities.waitOnFuture(migrationManager.pullSchemaFromAnyAvailableNode());
            logger.info("Local schema reset is complete.");
        });
    }

    @Override
    void waitUntilReadyForBootstrap()
    {
        migrationManager.waitUntilReadyForBootstrap();
    }

    @Override
    void onUpdatedSchemaVersion(InetAddressAndPort remote, UUID newSchemaVersionAsUUID, String reason)
    {
        // Not certain this can happen anymore but no harm done by ignoring in doubt ...
        if (newSchemaVersionAsUUID != null)
            migrationManager.maybeScheduleSchemaPull(newSchemaVersionAsUUID, remote, reason);
    }
}