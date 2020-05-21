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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.exceptions.RequestValidationException;

/**
 * Represents the "diff" of a schema changes, that is the schema metadata before and after the changes as well as
 * access to what changed.
 */
public class KeyspacesDiff
{
    private final Keyspaces before;
    private final Keyspaces after;

    final Keyspaces created;
    final Keyspaces dropped;
    final ImmutableList<KeyspaceMetadata.Diff> altered;

    private KeyspacesDiff(Keyspaces before,
                          Keyspaces after,
                          Keyspaces created,
                          Keyspaces dropped,
                          ImmutableList<KeyspaceMetadata.Diff> altered)
    {
        this.before = before;
        this.after = after;
        this.created = created;
        this.dropped = dropped;
        this.altered = altered;
    }

    static KeyspacesDiff diff(Keyspaces before, Keyspaces after)
    {
        if (before == after)
            return new KeyspacesDiff(before, after, Keyspaces.none(), Keyspaces.none(), ImmutableList.of());

        Keyspaces created = after.filter(k -> !before.containsKeyspace(k.name));
        Keyspaces dropped = before.filter(k -> !after.containsKeyspace(k.name));

        ImmutableList.Builder<KeyspaceMetadata.Diff> altered = ImmutableList.builder();
        before.forEach(keyspaceBefore ->
                       {
                           KeyspaceMetadata keyspaceAfter = after.getNullable(keyspaceBefore.name);
                           if (null != keyspaceAfter)
                               KeyspaceMetadata.diff(keyspaceBefore, keyspaceAfter).ifPresent(altered::add);
                       });

        return new KeyspacesDiff(before, after, created, dropped, altered.build());
    }

    /**
     * Whether this diff is empty, that is represents no change at all.
     */
    public boolean isEmpty()
    {
        return created.isEmpty() && dropped.isEmpty() && altered.isEmpty();
    }

    /**
     * The full definition of the keyspaces before or after the change.
     *
     * @param side whether to return the keyspaces before or after the change.
     * @return metadata for all keyspaces in the schema before or after the change based on {@code side}.
     */
    public Keyspaces keyspaces(TransformationSide side)
    {
        return side == TransformationSide.BEFORE ? before : after;
    }

    /**
     * The metadata of the provided keyspace before or after the change.
     *
     * @param side         whether the return the metadata before or after the change.
     * @param keyspaceName the name of the keyspace to return.
     * @return the metadata of {@code keyspaceName} before or after the change based on {@code side}. This can be
     * {@code null} if the keyspace didn't existed on this "side".
     */
    public KeyspaceMetadata keyspace(TransformationSide side, String keyspaceName)
    {
        return keyspaces(side).getNullable(keyspaceName);
    }

    /**
     * The metadata of the provided table before or after the change.
     *
     * @param side         whether the return the metadata before or after the change.
     * @param keyspaceName the name of the keyspace for the table to return.
     * @param tableName    the name of the table to return.
     * @return the metadata of {@code tableName} in {@code keyspaceName} before or after the change based on {@code
     * side}. This can be {@code null} if the table didn't existed on this "side".
     */
    public TableMetadata table(TransformationSide side, String keyspaceName, String tableName)
    {
        KeyspaceMetadata keyspaceMetadata = keyspace(side, keyspaceName);
        return keyspaceMetadata == null ? null : keyspaceMetadata.getTableOrViewNullable(tableName);
    }

    /**
     * Convenience method that, assuming the change this is the diff off altered one table and one table only, returns
     * the metadata of that table on the provided "side".
     *
     * @param side whether to return the metadata of the sole altered table before or after the change.
     * @return the metadata of the sole table altered by the change this is the diff of. This can never return
     * {@code null} but will throw {@link AssertionError} if the diff doesn't correspond to a change that altered
     * exactly one table.
     */
    public TableMetadata alteredTable(TransformationSide side)
    {
        assert altered.size() == 1;
        KeyspaceMetadata.Diff keyspaceDiff = altered.get(0);
        assert keyspaceDiff.tables.altered.size() == 1;
        Diff.Altered<TableMetadata> tableDiff = keyspaceDiff.tables.altered.iterator().next();
        return side == TransformationSide.BEFORE ? tableDiff.before : tableDiff.after;
    }

    /**
     * Returns the diff resulting from the application of the provided schema transformation to the provided
     * keyspaces (without applying anything).
     *
     * @param transformation the transformation to test.
     * @param before         the keyspaces to test-applySchemaMigration {@code transformation} on.
     * @return the diff of applying the provided transformation to the provided keyspaces.
     * @throws RequestValidationException if the provided transformation does not applySchemaMigration cleanly on top of the provided
     *                                    schema.
     */
    public static KeyspacesDiff testApply(SchemaTransformation transformation, Keyspaces before)
    {
        return diff(before, transformation.apply(before));
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("created", created)
                          .add("dropped", dropped)
                          .add("altered", altered)
                          .toString();
    }
}
