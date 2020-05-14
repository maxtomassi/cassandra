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

import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

public final class CreateFunctionStatement extends AbstractCreateFunctionStatement
{
    private final List<ColumnIdentifier> argumentNames;
    private final CQL3Type.Raw rawReturnType;
    private final boolean calledOnNullInput;
    private final String language;
    private final String body;

    public CreateFunctionStatement(String keyspaceName,
                                   String functionName,
                                   List<ColumnIdentifier> argumentNames,
                                   List<CQL3Type.Raw> rawArgumentTypes,
                                   CQL3Type.Raw rawReturnType,
                                   boolean calledOnNullInput,
                                   String language,
                                   String body,
                                   boolean orReplace,
                                   boolean ifNotExists)
    {
        super(keyspaceName, functionName, rawArgumentTypes, orReplace, ifNotExists);
        this.argumentNames = argumentNames;
        this.rawReturnType = rawReturnType;
        this.calledOnNullInput = calledOnNullInput;
        this.language = language;
        this.body = body;
    }

    // TODO: replace affected aggregates !!
    public Keyspaces apply(Keyspaces schema)
    {
        validateForApplication();

        UDFunction.assertUdfsEnabled(language);

        if (new HashSet<>(argumentNames).size() != argumentNames.size())
            throw ire("Duplicate argument names for given function %s with argument names %s", functionName, argumentNames);

        if (rawReturnType.isFrozen())
            throw ire("Return type '%s' cannot be frozen; remove frozen<> modifier from '%s'", rawReturnType, rawReturnType);

        KeyspaceMetadata keyspace = getExistingKeyspaceMetadata(schema);

        List<AbstractType<?>> argumentTypes = argumentsTypes(keyspace);
        AbstractType<?> returnType = rawReturnType.prepare(keyspaceName, keyspace.types).getType();

        UDFunction function =
            UDFunction.create(functionName,
                              argumentNames,
                              argumentTypes,
                              returnType,
                              calledOnNullInput,
                              language,
                              body);

        Function existingFunction = find(keyspace, argumentTypes);
        if (null != existingFunction)
        {
            if (existingFunction.isAggregate())
                throw ire("Function '%s' cannot replace an aggregate", functionName);

            if (ifNotExists)
                return schema;

            if (!orReplace)
                throw ire("Function '%s' already exists", functionName);

            if (calledOnNullInput != ((UDFunction) existingFunction).isCalledOnNullInput())
            {
                throw ire("Function '%s' must have %s directive",
                          functionName,
                          calledOnNullInput ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT");
            }

            if (!returnType.isCompatibleWith(existingFunction.returnType()))
            {
                throw ire("Cannot replace function '%s', the new return type %s is not compatible with the return type %s of existing function",
                          functionName,
                          returnType.asCQL3Type(),
                          existingFunction.returnType().asCQL3Type());
            }

            // TODO: update dependent aggregates
        }

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.functions.withAddedOrUpdated(function)));
    }

    @Override
    protected SchemaChange.Target schemaChangeTarget()
    {
        return Target.FUNCTION;
    }

    @Override
    public void authorize(ClientState client)
    {
        if (Schema.instance.findFunction(functionName, Lists.transform(rawArgumentTypes, t -> t.prepare(keyspaceName).getType())).isPresent() && orReplace)
            client.ensurePermission(Permission.ALTER, FunctionResource.functionFromCql(keyspaceName, functionName.name, rawArgumentTypes));
        else
            client.ensurePermission(Permission.CREATE, FunctionResource.keyspace(keyspaceName));
    }

    @Override
    protected AuditLogEntryType getAuditLogEntryType()
    {
        return AuditLogEntryType.CREATE_FUNCTION;
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, functionName.name);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final FunctionName name;
        private final List<ColumnIdentifier> argumentNames;
        private final List<CQL3Type.Raw> rawArgumentTypes;
        private final CQL3Type.Raw rawReturnType;
        private final boolean calledOnNullInput;
        private final String language;
        private final String body;
        private final boolean orReplace;
        private final boolean ifNotExists;

        public Raw(FunctionName name,
                   List<ColumnIdentifier> argumentNames,
                   List<CQL3Type.Raw> rawArgumentTypes,
                   CQL3Type.Raw rawReturnType,
                   boolean calledOnNullInput,
                   String language,
                   String body,
                   boolean orReplace,
                   boolean ifNotExists)
        {
            this.name = name;
            this.argumentNames = argumentNames;
            this.rawArgumentTypes = rawArgumentTypes;
            this.rawReturnType = rawReturnType;
            this.calledOnNullInput = calledOnNullInput;
            this.language = language;
            this.body = body;
            this.orReplace = orReplace;
            this.ifNotExists = ifNotExists;
        }

        public CreateFunctionStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.keyspace : state.getKeyspace();

            return new CreateFunctionStatement(keyspaceName,
                                               name.name,
                                               argumentNames,
                                               rawArgumentTypes,
                                               rawReturnType,
                                               calledOnNullInput,
                                               language,
                                               body,
                                               orReplace,
                                               ifNotExists);
        }
    }
}
