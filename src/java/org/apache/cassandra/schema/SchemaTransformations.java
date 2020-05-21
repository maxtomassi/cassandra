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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.Sets;

import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Factory and utility methods to create simple schema transformations.
 */
public class SchemaTransformations
{
    /**
     * Batch a number of transformations together.
     *
     * <p>Note that if any of the batched transformation throws, the whole batch will throw and nothing will be applied.
     *
     * @param transformations the transformation to batch. The resulting transformation will applySchemaMigration those transformation
     *                        in the passed order.
     * @return the created transformation.
     */
    public static SchemaTransformation batch(List<SchemaTransformation> transformations)
    {
        return schema ->
        {
            for (SchemaTransformation transformation : transformations)
                schema = transformation.apply(schema);
            return schema;
        };
    }

    /**
     * Creates a schema transformation that adds the provided keyspace (along with anything it may contain).
     *
     * @param keyspace the keyspace to add.
     * @return the created transformation. It will throw {@link AlreadyExistsException} on application if the provided
     * keyspace already exists.
     */
    public static SchemaTransformation createKeyspace(KeyspaceMetadata keyspace)
    {
        return createKeyspace(keyspace, false);
    }

    /**
     * Creates a schema transformation that adds the provided keyspace (along with anything it may contain) if it
     * doesn't already exists.
     *
     * @param keyspace the keyspace to add.
     * @return the created transformation. If, when applied, a keyspace of the same name than {@code keyspace} already
     * exists, the transformation is a no-op (in particular, if {@code keyspace} contains tables definition, those are
     * not added in that case, _even_ if the table don't exist).
     */
    public static SchemaTransformation createKeyspaceIfNotExists(KeyspaceMetadata keyspace)
    {
        return createKeyspace(keyspace, true);
    }

    private static SchemaTransformation createKeyspace(KeyspaceMetadata keyspace, boolean ignoreIfExists)
    {
        return schema ->
        {
            KeyspaceMetadata existing = schema.getNullable(keyspace.name);
            if (existing != null)
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(keyspace.name);
            }

            return schema.withAddedOrReplaced(keyspace);
        };
    }

    /**
     * Creates a schema transformation that drops the provided keyspace.
     *
     * @param keyspace the keyspace to remove.
     * @return the created transformation. It will throw {@link ConfigurationException} on application if the provided
     * keyspace does not exists.
     */
    public static SchemaTransformation dropKeyspace(String keyspace)
    {
        return dropKeyspace(keyspace, false);
    }

    /**
     * Creates a schema transformation that drops the provided keyspace if it exists.
     *
     * @param keyspace the keyspace to remove.
     * @return the created transformation. If upon application {@code keyspace} does not exists, it will be a no-op.
     */
    public static SchemaTransformation dropKeyspaceIfExists(String keyspace)
    {
        return dropKeyspace(keyspace, true);
    }

    private static SchemaTransformation dropKeyspace(String keyspace, boolean ignoreIfNotExists)
    {
        return schema ->
        {
            if (!schema.containsKeyspace(keyspace))
            {
                if (ignoreIfNotExists)
                    return schema;

                throw new ConfigurationException(String.format("Keyspace %s does not exists", keyspace));
            }

            return schema.without(keyspace);
        };
    }

    /**
     * Creates a schema transformation that replaces an existing keyspace with the provided instance.
     *
     * <p>Note that this is generally unsafe as no particular validation is done regarding the validity of replacing
     * the current keyspace metadata by the one provided, so it should be used with care when we can guarantee that
     * such change is, indeed, valid.
     *
     * <p>Also note that if the keyspace altered does not exists, nothing at all is done by the transformation (no error
     * is thrown).
     *
     * @param altered the metadata to use in replacement of the existing one.
     * @return the created transformation.
     */
    public static SchemaTransformation alterKeyspaceUnsafe(KeyspaceMetadata altered)
    {
        return schema ->
        {
            if (!schema.containsKeyspace(altered.name))
                return schema;

            return schema.withAddedOrReplaced(altered);
        };
    }

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
     * and later evolved on major upgrades (sometimes minor too). This method compares the current known definitions
     * of the tables (if the keyspace exists) to the expected, most modern ones expected by the running version of C*;
     * if any changes have been detected, a schema Mutation will be created which, when applied, should make
     * cluster's view of that keyspace aligned with the expected modern definition.
     *
     * @param keyspace   the expected modern definition of the keyspace
     * @param generation timestamp to use for the table changes in the schema mutation
     *
     * @return empty Optional if the current definition is up to date, or an Optional with the Mutation that would
     *         bring the schema in line with the expected definition.
     */
    public static SchemaTransformation evolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        Mutation.SimpleBuilder builder = null;

        KeyspaceMetadata definedKeyspace = SchemaManager.instance.getKeyspaceMetadata(keyspace.name);
        Tables definedTables = null == definedKeyspace ? Tables.none() : definedKeyspace.tables;

        for (TableMetadata table : keyspace.tables)
        {
            if (table.equals(definedTables.getNullable(table.name)))
                continue;

            if (null == builder)
            {
                // for the keyspace definition itself (name, replication, durability) always use generation 0;
                // this ensures that any changes made to replication by the user will never be overwritten.
                builder = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, 0);

                // now set the timestamp to generation, so the tables have the expected timestamp
                builder.timestamp(generation);
            }

            // for table definitions always use the provided generation; these tables, unlike their containing
            // keyspaces, are *NOT* meant to be altered by the user; if their definitions need to change,
            // the schema must be updated in code, and the appropriate generation must be bumped.
            SchemaKeyspace.addTableToSchemaMutation(table, true, builder);
        }

        SchemaTransformation transformation = builder == null ? SchemaTransformation

        return builder == null ? Optional.empty() : Optional.of(builder.build());
    }

    /**
     * Given the definition of a system distributed keyspace, creates a schema transformation that ensures that this
     * keyspace is part of the schema and up-to-date in it (contains everything defined in the provided one, modulo
     * potentially the tables from {@code tableNamesNotToUpdateIfExists}).
     *
     * <p>Please note that this method does not validate that, if any table is updated, that the previous table
     * definition and the new one are "compatible" (result from valid schema transformation), so this must be used
     * with care, when we know it's the case.
     *
     * @param keyspace the metadata of the keyspace as it should be after application.
     * @param tableNamesNotToUpdateIfExists by default, if a table from {@code keyspace} already exists, it is
     * "updated" (if necessary) to match the definition from {@code keyspace}. For tables of this array however, this
     * is not so and if the table exists, it is not updated and left to its existing state (but it is created if it
     * doesn't exists). This is to handle upgrade cases where updating the table definition should be pushed to a
     * specific time, which is handled by {@link SchemaUpgrade}.
     * @return the create transformation.
     */
    public static SchemaTransformation ensureSystemKeyspaceUpToDate(KeyspaceMetadata keyspace,
                                                                    String... tableNamesNotToUpdateIfExists)
    {
        // This should not be used for a user keyspace ever.
        assert SchemaConstants.isInternalKeyspace(keyspace.name) : keyspace.name + " is not an internal keyspace";

        Set<String> tablesNotToUpdate = Sets.newHashSet(tableNamesNotToUpdateIfExists);
        return schema -> {
            // Though we want to add or update any table the newly passed keyspace contains, we also carry on any
            // tables that exists before but are not in the new keyspace. The reason being that this is how some
            // DSE-side tests (that use SchemaTool.maybeCreateOrUpdateKeyspace) expected this to behave. Alternatively,
            // we could imagine to "fix" those call-sites, but in practice, there is no reason we would want to remove
            // a system table (even if we deprecate one someday, we'll probably want special treatment), so this feel
            // reasonable reasonable behavior.
            KeyspaceMetadata toAdd = keyspace;
            KeyspaceMetadata previous = schema.getNullable(keyspace.name);
            if (previous != null)
            {
                // If the keyspace already exists, we preserve whatever parameters it has. While a bit random, this
                // is legacy behavior, and without that, user modification of system keyspace RF does not work
                // properly (in large parts because the 'preserveExistingSetting' behavior does not work correctly
                // since DB-2296).
                // TODO: we should probably fix all this at some point.
                toAdd = toAdd.withSwapped(previous.params);

                for (TableMetadata previousTable : previous.tables)
                {
                    TableMetadata newTable = toAdd.tables.getNullable(previousTable.name);
                    if (newTable == null)
                    {
                        toAdd = toAdd.withSwapped(toAdd.tables.with(previousTable));
                    }
                    else if (tablesNotToUpdate.contains(previousTable.name))
                    {
                        // Replace by the existing definition if asked.
                        // In theory, both tables should have the same ID, but given we're dealing with internal tables,
                        // it's possible we mess up and not generate the ID consistently (through TableId.forSystemTable
                        // typically), or somehow change how it's generate across versions. At a minimum, the
                        // schemaUpgrade() test of CassandraAuditWriterTest ends up doing this, and while the test is
                        // not really realistic and we could just "fix" it, it's not hard to be resilient to such
                        // things.
                        // Anyway, if the ID are not equals, the withSwappedTable() would not work properly, so the way
                        // around it is to remove the version of the table we don't want first, and add the one we
                        // want second. Note that in theory, that method is less safe than withSwappedTable since the
                        // later deals properly with views or graph labels that may depend on the table, but as we
                        // know this only run for internal keyspaces, this is probably not an issue in practice.
                        if (newTable.id.equals(previousTable.id))
                        {
                            toAdd = toAdd.withSwappedTable(previousTable);
                        }
                        else
                        {
                            toAdd = toAdd.withSwapped(toAdd.tables.without(newTable));
                            toAdd = toAdd.withSwapped(toAdd.tables.with(previousTable));
                        }
                    }
                    else
                    {
                        // We may have to applySchemaMigration a few update to the "new" table based on the existing definition,
                        // namely:
                        // - add any column the previous table add, but not the new one. This is done because at
                        //   least CassandraAuditWriterTest#schemaMoreColumns expect it, commenting that this was to
                        //   avoid removing columns; this should only be useful if said columns are added in minor
                        //   versions though, since otherwise, the schema of newer DSE versions won't be propagated
                        //   to lower version DSE anyway. On the flip side, if someone downgrade a node after a failed
                        //   upgrade, this will prevent the table to remove the added columns, which is probably
                        //   innocuous, but somewhat unexpected. Anyway, we may want to re-evaluate this at some point.
                        // - As mentioned in the previous branch, it's possible the table ID changed on upgrade. In that
                        //   case, we want to preserve said ID: if we didn't this would be equivalent to dropping then
                        //   re-creating the table, and the drop would lose all existing data

                        toAdd = toAdd.withSwapped(toAdd.tables.without(newTable));

                        TableMetadata.Builder updatedBuilder = newTable.unbuild();

                        if (!newTable.id.equals(previousTable.id))
                            updatedBuilder.id(previousTable.id);

                        for (ColumnMetadata column : previousTable.regularAndStaticColumns())
                        {
                            if (!newTable.regularAndStaticColumns().contains(column))
                                updatedBuilder.addColumn(column);
                        }

                        toAdd = toAdd.withSwapped(toAdd.tables.with(updatedBuilder.build()));
                    }
                }
            }
            return schema.withAddedOrReplaced(toAdd);
        };
    }

    /**
     * Creates a schema transformation that adds the provided table.
     *
     * @param table the table to add.
     * @return the created transformation. It will throw {@link AlreadyExistsException} on application if the provided
     * table already exists.
     */
    public static SchemaTransformation createTable(TableMetadata table)
    {
        return createTable(table, false);
    }

    /**
     * Creates a schema transformation that adds the provided table if it doesn't already exists.
     *
     * @param table the table to add.
     * @return the created transformation. If, upon application, the table {@code table} already, this is a no-op.
     */
    public static SchemaTransformation createTableIfNotExists(TableMetadata table)
    {
        return createTable(table, true);
    }

    private static SchemaTransformation createTable(TableMetadata table, boolean ignoreIfExists)
    {
        return schema ->
        {
            KeyspaceMetadata keyspace = schema.getNullable(table.keyspace);
            if (keyspace == null)
                throw invalidRequest("Keyspace '%s' doesn't exist", table.keyspace);

            if (keyspace.hasTable(table.name))
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(table.keyspace, table.name);
            }

            return schema.withAddedOrReplaced(keyspace.withSwapped(keyspace.tables.with(table)));
        };
    }

    /**
     * Creates a schema transformation that adds a table given its CQL definition.
     *
     * <p>This method assumes no default keyspace set, so the query must use a fully qualified table name. Use
     * {@link #createTable(String, String)} otherwise.
     *
     * @param query the CREATE TABLE query defining the table.
     * @return the created transformation. Whether or not the transformation throws on application if the table
     * already exists depends on the presence of IF NOT EXISTS in the query.
     */
    public static SchemaTransformation createTable(String query)
    {
        return createTable(null, query);
    }

    /**
     * Creates a schema transformation that adds a table given its CQL definition (and using the provided default
     * keyspace).
     *
     * @param defaultKeyspace the default keyspace to use when parsing the query.
     * @param query the CREATE TABLE query defining the table.
     * @return the created transformation. Whether or not the transformation throws on application if the table
     * already exists depends on the presence of IF NOT EXISTS in the query.
     */
    public static SchemaTransformation createTable(String defaultKeyspace, String query)
    {
        CreateTableStatement statement = CreateTableStatement.parseStatement(query, defaultKeyspace);
        // We may need the "current" schema types (and graph labels) to properly build the table metadata, so the rest
        // has to happen within the transformation.
        return schema ->
        {
            KeyspaceMetadata keyspaceMetadata = schema.getNullable(statement.keyspace());
            if (keyspaceMetadata == null)
                throw invalidRequest("Keyspace %s does not exists", statement.keyspace());

            if (keyspaceMetadata.tables.get(statement.table()).isPresent())
            {
                if (statement.hasIfNotExists())
                    return schema;

                throw invalidRequest("Table %s already exists in keyspace %s",
                                     statement.table(), statement.keyspace());
            }

            TableMetadata newTable = statement.builder(keyspaceMetadata.types)
                                              .build();

            return schema.withAddedOrReplaced(keyspaceMetadata.withSwapped(keyspaceMetadata.tables.with(newTable)));
        };
    }

    /**
     * Creates a schema transformation that alters a table (or view).
     *
     * @param keyspaceName the keyspace of the table to alter.
     * @param tableName the name of the table to alter.
     * @param alteration the actual alteration to make, in the form of a consumer that is called with a
     *                   {@link TableMetadata.Builder} populated by the existing version of the table and to which
     *                   modification can be made.
     * @return the created transformation.
     */
    public static SchemaTransformation alterTable(String keyspaceName,
                                                  String tableName,
                                                  Consumer<TableMetadata.BaseBuilder<?, ?>> alteration)
    {
        return schema ->
        {
            KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
            if (keyspace == null)
                throw invalidRequest("Keyspace '%s' doesn't exist", keyspaceName);

            TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
            if (metadata == null)
                throw invalidRequest("Table '%s.%s' doesn't exist", keyspaceName, tableName);

            TableMetadata.BaseBuilder<?, ?> altered = metadata.unbuild();
            alteration.accept(altered);
            return schema.withAddedOrReplaced(keyspace.withSwappedTable(altered.build()));
        };
    }

    /**
     * Creates a schema transformation that drops the provided table.
     *
     * @param table the table to remove.
     * @return the created transformation. The transformation will throw a {@link ConfigurationException}  on
     * application if a table of the same name as {@code table} does not exists. Otherwise, said table is dropped,
     * regardless of whether it's exact definition matches the provided one (that is, only the table name is matched).
     */
    public static SchemaTransformation dropTable(TableMetadata table)
    {
        return dropTable(table.keyspace, table.name);
    }

    /**
     * Creates a schema transformation that drops the provided table.
     *
     * @param keyspaceName the name of the keyspace for the table to remove.
     * @param tableName the name of the table to remove.
     * @return the created transformation. The transformation will throw a {@link ConfigurationException}  on
     * application if the table does not exists.
     */
    public static SchemaTransformation dropTable(String keyspaceName, String tableName)
    {
        return dropTable(keyspaceName, tableName, false);
    }

    /**
     * Creates a schema transformation that drops the provided table if it exists.
     *
     * @param keyspaceName the name of the keyspace for the table to remove.
     * @param tableName the name of the table to remove.
     * @return the created transformation. It is a no-op if upon application the provided table does not exists.
     */
    public static SchemaTransformation dropTableIfExists(String keyspaceName, String tableName)
    {
        return dropTable(keyspaceName, tableName, true);
    }

    private static SchemaTransformation dropTable(String keyspaceName, String tableName, boolean ignoreIfNotExists)
    {
        return schema ->
        {
            KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
            if (keyspace == null)
                throw invalidRequest("Keyspace '%s' doesn't exist", keyspaceName);

            if (!keyspace.hasTable(tableName))
            {
                if (ignoreIfNotExists)
                    return schema;

                throw new ConfigurationException(String.format("Table %s.%s does not exists", keyspaceName, tableName));
            }

            return schema.withAddedOrReplaced(keyspace.withSwapped(keyspace.tables.without(tableName)));
        };
    }

    /**
     * Creates a schema transformation that adds the provided user type.
     *
     * @param udt the user types to add.
     * @return the created transformation. If upon application the type already exists, the transformation
     * throws a {@link ConfigurationException}.
     */
    public static SchemaTransformation createType(UserType udt)
    {
        return createTypes(Types.of(udt));
    }

    /**
     * Creates a schema transformation that adds all the provided user types.
     *
     * @param toAdd the user types to add.
     * @return the created transformation. If upon application _any_ of the types already exists, the transformation
     * throws a {@link ConfigurationException}.
     */
    public static SchemaTransformation createTypes(Types toAdd)
    {
        return createTypes(toAdd, false);
    }

    /**
     * Creates a schema transformation that adds all of the provided user types that don't already exists.
     *
     * @param toAdd the user types to add.
     * @return the created transformation. Upon application, any of the user types that already exists are simply
     * ignored (and so, if all already exists, this is a no-op).
     */
    public static SchemaTransformation createTypesIfNotExists(Types toAdd)
    {
        return createTypes(toAdd, true);
    }

    private static SchemaTransformation createTypes(Types toAdd, boolean ignoreIfExists)
    {
        return schema ->
        {
            if (toAdd.isEmpty())
                return schema;

            String keyspaceName = toAdd.iterator().next().keyspace;
            KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
            if (null == keyspace)
                throw invalidRequest("Keyspace '%s' doesn't exist", keyspaceName);

            Types types = keyspace.types;
            for (UserType type : toAdd)
            {
                if (types.containsType(type.name))
                {
                    if (ignoreIfExists)
                        continue;

                    throw new ConfigurationException("Type " + type + " already exists in " + keyspaceName);
                }

                types = types.with(type);
            }
            return schema.withAddedOrReplaced(keyspace.withSwapped(types));
        };
    }

//    /**
//     * Creates a schema transformation that creates a type given its CQL definition.
//     *
//     * <p>This method assumes no default keyspace set, so the query must use a fully qualified type name. Use
//     * {@link #createType(String, String)} otherwise.
//     *
//     * @param query the CREATE TYPE query defining the type.
//     * @return the created transformation. Whether or not the transformation throws on application if the table
//     * already exists depends on the presence of IF NOT EXISTS in the query.
//     */
//    public static SchemaTransformation createType(String query)
//    {
//        return createType(null, query);
//    }
//
//    /**
//     * Creates a schema transformation that creates a type given its CQL definition (and using the provided default
//     * keyspace).
//     *
//     * @param defaultKeyspace the default keyspace to use when parsing the query.
//     * @param query the CREATE TYPE query defining the type.
//     * @return the created transformation. Whether or not the transformation throws on application if the table
//     * already exists depends on the presence of IF NOT EXISTS in the query.
//     */
//    public static SchemaTransformation createType(String defaultKeyspace, String query)
//    {
//        CreateTypeStatement statement = CreateTypeStatement.parseStatement(query, defaultKeyspace);
//        // We may need the "current" schema types to properly build the new type, so the rest has to happen within the
//        // transformation.
//        return schema ->
//        {
//            KeyspaceMetadata keyspaceMetadata = schema.getNullable(statement.keyspace());
//            if (keyspaceMetadata == null)
//                throw invalidRequest("Keyspace %s does not exists", statement.keyspace());
//
//            if (keyspaceMetadata.types.get(statement.rawTypeName()).isPresent())
//            {
//                if (statement.hasIfNotExists())
//                    return schema;
//
//                throw invalidRequest("Type %s already exists in keyspace %s",
//                                     statement.typeName(), statement.keyspace());
//            }
//
//            UserType newType = statement.createType(keyspaceMetadata.types);
//
//            return schema.withAddedOrReplaced(keyspaceMetadata.withSwapped(keyspaceMetadata.types.with(newType)));
//        };
//    }

    /**
     * Creates a schema transformation that adds the provided view.
     *
     * @param view the view to add.
     * @return the created transformation. It will throw {@link AlreadyExistsException} on application if the provided
     * table already exists.
     */
    public static SchemaTransformation createView(ViewMetadata view)
    {
        return createView(view, false);
    }

    /**
     * Creates a schema transformation that adds the provided view if it doesn't already exists.
     *
     * @param view the view to add.
     * @return the created transformation. If, upon application, the view {@code view} already, this is a no-op.
     */
    public static SchemaTransformation createViewfNotExists(ViewMetadata view)
    {
        return createView(view, true);
    }

    private static SchemaTransformation createView(ViewMetadata view, boolean ignoreIfExists)
    {
        return schema ->
        {
            TableMetadata viewTable = view.metadata;
            KeyspaceMetadata keyspace = schema.getNullable(viewTable.keyspace);
            if (keyspace == null)
                throw invalidRequest("Cannot add view to non existing keyspace '%s'", viewTable.keyspace);

            if (keyspace.hasView(viewTable.name))
            {
                if (ignoreIfExists)
                    return schema;

                throw new AlreadyExistsException(viewTable.keyspace, viewTable.name);
            }

            viewTable.validate();

            return schema.withAddedOrReplaced(keyspace.withSwapped(keyspace.views.with(view)));
        };
    }

    /**
     * Creates a schema transformation that adds the provided function in the provided keyspace.
     *
     * @return the created transformation. It will throw {@link InvalidRequestException} on application if the provided
     * function already exists.
     */
    public static SchemaTransformation createFunction(String keyspace, Function function)
    {
        return schema -> {
            KeyspaceMetadata keyspaceMetadata = schema.getNullable(keyspace);
            if (keyspaceMetadata == null)
                throw invalidRequest("Keyspace " + keyspace + " does not exists");

            if (keyspaceMetadata.functions.find(function.name(), function.argTypes()).isPresent())
                throw invalidRequest("Function " + function + " already exists");

            return schema.withAddedOrReplaced(keyspaceMetadata.withSwapped(keyspaceMetadata.functions.with(function)));
        };
    }

    /**
     * Creates a schema transformation that drop the provided function in the provided keyspace.
     *
     * @return the created transformation. It will throw {@link InvalidRequestException} on application if the provided
     * function does not exists.
     */
    public static SchemaTransformation dropFunction(String keyspace, Function function)
    {
        return schema -> {
            KeyspaceMetadata keyspaceMetadata = schema.getNullable(keyspace);
            if (keyspaceMetadata == null)
                throw invalidRequest("Keyspace " + keyspace + " does not exists");

            if (!keyspaceMetadata.functions.find(function.name(), function.argTypes()).isPresent())
                throw invalidRequest("Function " + function + " does not exists");

            // Not using the Functions#without(Function) method below as it checks for reference equality and don't want
            // to risk that not working for tests.
            return schema.withAddedOrReplaced(
            keyspaceMetadata.withSwapped(
            keyspaceMetadata.functions.without(function.name(), function.argTypes())));
        };
    }
}
