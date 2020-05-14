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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.SchemaTransformation;
import org.apache.cassandra.service.QueryState;

abstract class AbstractFunctionStatement extends AlterSchemaStatement
{
    protected final FunctionName functionName;
    protected final List<CQL3Type.Raw> rawArgumentTypes;

    AbstractFunctionStatement(String keyspaceName,
                              String functionName,
                              List<CQL3Type.Raw> rawArgumentTypes)
    {
        super(keyspaceName);
        this.functionName = new FunctionName(keyspaceName, functionName);
        this.rawArgumentTypes = rawArgumentTypes;
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(getAuditLogEntryType(), functionName.keyspace, functionName.name);
    }

    protected abstract AuditLogEntryType getAuditLogEntryType();

    /**
     * Performs validation on the function statement that should be done pre-application.
     * <p>
     * This should be called by subclasses at the beginning of their {@link #apply(Keyspaces)} method.
     * <p>
     * The only reason we don't do this in {@link #validate} is to be consistent with all other schema statements, as
     * schema statements do their validation as part of {@link #apply(Keyspaces)}. And the reason for that is that
     * schema statements are {@link SchemaTransformation}, which implicitly assume {@link #apply(Keyspaces)} does
     * ensures the validation of the transformation (besides, many validation does require knowing on top of which
     * schema the transformation applies, so cannot be done in {@link #validate(QueryState)}).
     */
    protected void validateForApplication()
    {
        rawArgumentTypes.stream()
                        .filter(CQL3Type.Raw::isFrozen)
                        .findFirst()
                        .ifPresent(t -> { throw ire("Argument '%s' cannot be frozen; remove frozen<> modifier from '%s'", t, t); });
    }

    protected List<AbstractType<?>> argumentsTypes(KeyspaceMetadata keyspace)
    {
        return rawArgumentTypes.stream()
                               .map(t -> t.prepare(keyspaceName, keyspace.types).getType())
                               .collect(Collectors.toList());
    }

    protected Function find(KeyspaceMetadata keyspace, List<AbstractType<?>> argumentTypes)
    {
        return keyspace.functions.find(functionName, argumentTypes).orElse(null);
    }
}
