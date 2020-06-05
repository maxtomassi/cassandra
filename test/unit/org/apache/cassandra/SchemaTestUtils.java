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

package org.apache.cassandra;

import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.schema.SchemaTransformations.alterTable;

/**
 * Simple test helpers/convenient shortcuts to deal with schema creation and update.
 *
 * <p>Those methods come in complement to {@link SchemaTransformations} that already provide the main schema updates.
 * This provide a few more specific methods useful when testing.
 *
 * <p>All of the methods are name in such a way that importing them statically is, in the context of tests, a reasonable
 * idea, though many of them conflicts with ones of {@link CQLTester}, so they can effectively only be imported
 * statically when not used in conjunction with {@link CQLTester}.
 *
 * TODO: That conflict with CQLTester highlight that our testing tools don't compose too elegantly today and we should
 *   improve on this someday.
 */
public class SchemaTestUtils
{
    /**
     * Apply the provided schema transformations (in the provided order).
     *
     * @param transformations the transformation to apply.
     */
    public static void doSchemaChanges(SchemaTransformation... transformations)
    {
        doSchemaChanges(Arrays.asList(transformations));
    }

    /**
     * Apply the provided schema transformations (in the list order).
     *
     * @param transformations the transformation to apply.
     */
    public static void doSchemaChanges(List<SchemaTransformation> transformations)
    {
        SchemaManager.instance.apply(SchemaTransformations.batch(transformations));
    }

    /**
     * Creates an empty keyspace with RF=1 (and the simple replication strategy).
     *
     * <p>This method throws if the keyspace already exists.
     */
    public static SchemaTransformation createKeyspace(String name)
    {
        return createKeyspace(name, 1);
    }

    /**
     * Creates an empty keyspace with the provided replication factor (and the simple replication strategy).
     *
     * <p>This method throws if the keyspace already exists.
     */
    public static SchemaTransformation createKeyspace(String name, int replicationFactor)
    {
        return createKeyspace(name, KeyspaceParams.simple(replicationFactor));
    }

    /**
     * Creates an empty keyspace with the provided keyspace parameters.
     *
     * <p>This method throws if the keyspace already exists.
     */
    public static SchemaTransformation createKeyspace(String name, KeyspaceParams params)
    {
        return SchemaTransformations.createKeyspace(KeyspaceMetadata.create(name, params));
    }

    /**
     * Creates an empty keyspace with RF=1 (and the simple replication strategy) if it doesn't already exists.
     */
    public static SchemaTransformation createKeyspaceIfNotExists(String name)
    {
        return createKeyspaceIfNotExists(name, 1);
    }

    /**
     * Creates an empty keyspace with the provided replication factor (and the simple replication strategy) if it
     * doesn't already exists.
     */
    public static SchemaTransformation createKeyspaceIfNotExists(String name, int replicationFactor)
    {
        return createKeyspaceIfNotExists(name, KeyspaceParams.simple(replicationFactor));
    }

    /**
     * Creates an empty keyspace with the provided keyspace parameters if it doesn't already exists.
     */
    public static SchemaTransformation createKeyspaceIfNotExists(String name, KeyspaceParams params)
    {
        return SchemaTransformations.createKeyspaceIfNotExists(KeyspaceMetadata.create(name, params));
    }

    //
    // A few specific table alterations that are convenient
    //

    /**
     * Creates a transformation that update the GC grace of the provided table.
     */
    public static SchemaTransformation updateGCGrace(TableMetadata table, int newValue)
    {
        return updateGCGrace(table.keyspace, table.name, newValue);
    }

    /**
     * Creates a transformation that update the GC grace of the provided table.
     */
    public static SchemaTransformation updateGCGrace(String keyspaceName, String tableName, int newValue)
    {
        return alterTable(keyspaceName, tableName, b -> b.gcGraceSeconds(newValue));
    }

    /**
     * Adds the provided index to the provided existing table.
     */
    public static SchemaTransformation addIndex(TableMetadata table, IndexMetadata newIndex)
    {
        return alterTable(table.keyspace, table.name, b -> b.addIndex(newIndex));
    }

    /**
     * Removes the provided index to the provided existing table.
     */
    public static SchemaTransformation dropIndex(TableMetadata table, String indexName)
    {
        return SchemaTransformations.alterTable(table.keyspace, table.name, b -> b.removeIndex(indexName));
    }
}
