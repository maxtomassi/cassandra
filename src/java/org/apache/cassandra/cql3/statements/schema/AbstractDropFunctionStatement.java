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

package org.apache.cassandra.cql3.statements.schema;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;

import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TransformationSide;
import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;
import static java.lang.String.join;

import static com.google.common.collect.Iterables.transform;

abstract class AbstractDropFunctionStatement extends AbstractFunctionStatement
{
    protected final boolean argumentsSpecified;
    protected final boolean ifExists;

    AbstractDropFunctionStatement(String keyspaceName,
                                  String functionName,
                                  List<CQL3Type.Raw> rawArgumentTypes,
                                  boolean argumentsSpecified,
                                  boolean ifExists)
    {
        super(keyspaceName, functionName, rawArgumentTypes);
        this.argumentsSpecified = argumentsSpecified;
        this.ifExists = ifExists;
    }

    protected abstract String functionTypeName();

    protected abstract Predicate<Function> functionTypeFilter();

    protected abstract void checkDropValid(KeyspaceMetadata keyspace, Function toDrop);

    protected Function dropped(KeyspacesDiff diff)
    {
        KeyspaceMetadata keyspaceBefore = diff.keyspace(TransformationSide.BEFORE, keyspaceName);
        if (argumentsSpecified)
        {
            List<AbstractType<?>> argTypes = argumentsTypes(keyspaceBefore);
            return find(keyspaceBefore, argTypes);
        }
        else
        {
            Collection<Function> functions = keyspaceBefore.functions.get(functionName);
            // There should be only one, or the apply would have failed somewhere
            return Iterables.getOnlyElement(functions);
        }
    }

    protected String name()
    {
        return argumentsSpecified
               ? format("%s.%s(%s)", keyspaceName, functionName.name, join(", ", transform(rawArgumentTypes, CQL3Type.Raw::toString)))
               : format("%s.%s", keyspaceName, functionName.name);
    }

    @Override
    public Keyspaces apply(Keyspaces schema)
    {
        validateForApplication();

        String name = name();

        KeyspaceMetadata keyspace = getKeyspaceMetadata(schema, !ifExists);

        // Can only happen if ifExists but the keyspace doesn't exist (note that this is inconsistent with other
        // statements like CreateTable/CreateType/CreateIndex/etc..., where we throw if the keyspace does not exists
        // even with 'IF EXISTS' (which thus only apply to the object created itself). This is a pre-existing
        // inconsistency however so preserving it for now).
        if (keyspace == null)
            return schema;

        Collection<Function> functions = keyspace.functions.get(functionName);
        if (functions.size() > 1 && !argumentsSpecified)
        {
            throw ire("'DROP %s %s' matches multiple function definitions; " +
                      "specify the argument types by issuing a statement like " +
                      "'DROP %s %s (type, type, ...)'. You can use cqlsh " +
                      "'DESCRIBE %s %s' command to find all overloads",
                      functionTypeName(), functionName, functionTypeName(), functionName, functionTypeName(), functionName);
        }

        List<AbstractType<?>> argumentTypes = argumentsTypes(keyspace);

        Predicate<Function> filter = functionTypeFilter();
        if (argumentsSpecified)
            filter = filter.and(f -> Functions.typesMatch(f.argTypes(), argumentTypes));

        Function function = functions.stream().filter(filter).findAny().orElse(null);
        if (null == function)
        {
            if (ifExists)
                return schema;

            throw ire("%s '%s' doesn't exist", lowerCaseExceptFirstLetter(functionTypeName()), name);
        }

        checkDropValid(keyspace, function);

        return schema.withAddedOrReplaced(keyspace.withSwapped(keyspace.functions.without(function)));
    }

    @Override
    public void authorize(ClientState client)
    {
        KeyspaceMetadata keyspace = SchemaManager.instance.getKeyspaceMetadata(keyspaceName);
        if (null == keyspace)
            return;

        Stream<Function> functions = keyspace.functions.get(functionName).stream();
        if (argumentsSpecified)
            functions = functions.filter(f -> Functions.typesMatch(f.argTypes(), argumentsTypes(keyspace)));

        functions.forEach(f -> client.ensurePermission(Permission.DROP, FunctionResource.function(f)));
    }

    private static String lowerCaseExceptFirstLetter(String name)
    {
        return name.charAt(0) + name.substring(1).toLowerCase();
    }
}