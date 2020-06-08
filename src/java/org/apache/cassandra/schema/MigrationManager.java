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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.concurrent.Stage.MIGRATION;
import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;

/**
 * Handles "migrations" (PUSH, PULL) for the schema code.
 * <p>
 * Migrations refer to the PUSH of schema changes from the schema update coordinator to other nodes, and the PULL
 * of schema triggered when the local node notices its schema version differs from that of another node.
 */
public class MigrationManager
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    private static final int MIGRATION_DELAY_IN_MS = 60000;

    private static final int MIGRATION_TASK_WAIT_IN_SECONDS = Integer.parseInt(System.getProperty("cassandra.migration_task_wait_in_seconds", "1"));

    private final DefaultSchemaUpdateHandler updateHandler;

    MigrationManager(DefaultSchemaUpdateHandler updateHandler)
    {
        this.updateHandler = updateHandler;
    }

    /**
     * Apply a schema migration received from a remote node.
     *
     * @param mutations the mutations of the schema migration to apply
     * @return a future on the application of the migration.
     */
    public void apply(Collection<Mutation> mutations)
    {
        updateHandler.applySchemaMigration(mutations);
    }

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to applySchemaMigration locally.
     */
    void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddressAndPort endpoint, String releaseVersion)
    {
        String ourMajorVersion = FBUtilities.getReleaseVersionMajor();
        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema because release version in Gossip is not major version {}, it is {}", ourMajorVersion, releaseVersion);
            return;
        }
        if (SchemaManager.instance.getVersionAsUUID() == null)
        {
            logger.debug("Not pulling schema from {}, because local schema version is not known yet",
                         endpoint);
            SchemaMigrationDiagnostics.unknownLocalSchemaVersion(endpoint, theirVersion);
            return;
        }
        if (SchemaManager.instance.isSameVersion(theirVersion))
        {
            logger.debug("Not pulling schema from {}, because schema versions match ({})",
                         endpoint,
                         schemaVersionToString(theirVersion));
            SchemaMigrationDiagnostics.versionMatch(endpoint, theirVersion);
            return;
        }
        if (!shouldPullSchemaFrom(endpoint))
        {
            logger.debug("Not pulling schema from {}, because versions match ({}/{}), or shouldPullSchemaFrom returned false",
                         endpoint, SchemaManager.instance.getVersionAsUUID(), theirVersion);
            SchemaMigrationDiagnostics.skipPull(endpoint, theirVersion);
            return;
        }

        if (SchemaManager.instance.isEmpty() || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         schemaVersionToString(SchemaManager.instance.getVersionAsUUID()),
                         schemaVersionToString(theirVersion));
            submitMigrationTask(endpoint);
        }
        else
        {
            // Include a delay to make sure we have a chance to applySchemaMigration any changes being
            // pushed out simultaneously. See CASSANDRA-5025
            Runnable runnable = () ->
            {
                // grab the latest version of the schema since it may have changed again since the initial scheduling
                UUID epSchemaVersion = Gossiper.instance.getSchemaVersion(endpoint);
                if (epSchemaVersion == null)
                {
                    logger.debug("epState vanished for {}, not submitting migration task", endpoint);
                    return;
                }
                if (SchemaManager.instance.isSameVersion(epSchemaVersion))
                {
                    logger.debug("Not submitting migration task for {} because our versions match ({})", endpoint, epSchemaVersion);
                    return;
                }
                logger.debug("Submitting migration task for {}, schema version mismatch: local={}, remote={}",
                             endpoint,
                             schemaVersionToString(SchemaManager.instance.getVersionAsUUID()),
                             schemaVersionToString(epSchemaVersion));
                submitMigrationTask(endpoint);
            };
            ScheduledExecutors.nonPeriodicTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
    }

    private Future<?> submitMigrationTask(InetAddressAndPort endpoint)
    {
        /*
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        return MIGRATION.submit(new MigrationTask(endpoint, this::apply));
    }

    static boolean shouldPullSchemaFrom(InetAddressAndPort endpoint)
    {
        /*
         * Don't request schema from ourselves.
         * Don't request schema from nodes with a differnt or unknonw major version (may have incompatible schema)
         * Don't request schema from fat clients
         */
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
                && MessagingService.instance().versions.knows(endpoint)
                && MessagingService.instance().versions.getRaw(endpoint) == MessagingService.current_version
                && !Gossiper.instance.isGossipOnlyMember(endpoint);
    }

    Future<?> pullSchemaFromAnyAvailableNode()
    {
        Optional<InetAddressAndPort> pullFrom = Gossiper.instance.getLiveMembers()
                                                          .stream()
                                                          .filter(MigrationManager::shouldPullSchemaFrom)
                                                          .findFirst();

        if (!pullFrom.isPresent())
            return CompletableFuture.completedFuture(null);

        return submitMigrationTask(pullFrom.get());
    }

    private static boolean shouldPushSchemaTo(InetAddressAndPort endpoint)
    {
        // only push schema to nodes with known and equal versions
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
               && MessagingService.instance().versions.knows(endpoint)
               && MessagingService.instance().versions.getRaw(endpoint) == MessagingService.current_version;
    }

    void waitUntilReadyForBootstrap()
    {
        CountDownLatch completionLatch;
        while ((completionLatch = MigrationTask.getInflightTasks().poll()) != null)
        {
            try
            {
                if (!completionLatch.await(MIGRATION_TASK_WAIT_IN_SECONDS, TimeUnit.SECONDS))
                    logger.error("Migration task failed to complete");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                logger.error("Migration task was interrupted");
            }
        }
    }

    /**
     * Converts the given schema version to a string. Returns {@code unknown}, if {@code version} is {@code null}
     * or {@code "(empty)"}, if {@code version} refers to an empty schema.
     */
    static String schemaVersionToString(UUID version)
    {
        if (version == null)
            return "unknown";
        if (DefaultSchema.EMPTY_VERSION.equals(version))
            return "(empty)";
        return version.toString();
    }

    void pushMigrationToOtherNodes(Collection<Mutation> schemaTransformation)
    {
        Gossiper.instance.getLiveMembers()
                         .stream()
                         .filter(MigrationManager::shouldPushSchemaTo)
                         .forEach(endpoint -> pushSchemaMutation(endpoint, schemaTransformation));
    }

    private static void pushSchemaMutation(InetAddressAndPort endpoint, Collection<Mutation> schemaTransformation)
    {
        logger.debug("Pushing schema to endpoint {}", endpoint);
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, schemaTransformation);
        MessagingService.instance().send(message, endpoint);
    }

    public static class MigrationsSerializer implements IVersionedSerializer<Collection<Mutation>>
    {
        public static MigrationsSerializer instance = new MigrationsSerializer();

        public void serialize(Collection<Mutation> schema, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(schema.size());
            for (Mutation mutation : schema)
                Mutation.serializer.serialize(mutation, out, version);
        }

        public Collection<Mutation> deserialize(DataInputPlus in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<Mutation> schema = new ArrayList<>(count);

            for (int i = 0; i < count; i++)
                schema.add(Mutation.serializer.deserialize(in, version));

            return schema;
        }

        public long serializedSize(Collection<Mutation> schema, int version)
        {
            int size = TypeSizes.sizeof(schema.size());
            for (Mutation mutation : schema)
                size += mutation.serializedSize(version);
            return size;
        }
    }
}
