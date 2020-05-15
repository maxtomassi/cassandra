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
import java.util.function.Predicate;

import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDAggregate;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

public final class DropAggregateStatement extends AbstractDropFunctionStatement
{
    public DropAggregateStatement(String keyspaceName,
                                  String aggregateName,
                                  List<CQL3Type.Raw> arguments,
                                  boolean argumentsSpecified,
                                  boolean ifExists)
    {
        super(keyspaceName, aggregateName, arguments, argumentsSpecified, ifExists);
    }

    @Override
    protected String functionTypeName()
    {
        return "AGGREGATE";
    }

    @Override
    protected Predicate<Function> functionTypeFilter()
    {
        return Functions.Filter.UDA;
    }

    @Override
    protected void checkDropValid(KeyspaceMetadata keyspace, Function toDrop)
    {
        // Nothing specific to check
    }

    @Override
    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return SchemaChange.forAggregate(Change.DROPPED, (UDAggregate) dropped(diff));
    }

    @Override
    protected AuditLogEntryType getAuditLogEntryType()
    {
        return AuditLogEntryType.DROP_AGGREGATE;
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, functionName.name);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final FunctionName name;
        private final List<CQL3Type.Raw> arguments;
        private final boolean argumentsSpecified;
        private final boolean ifExists;

        public Raw(FunctionName name,
                   List<CQL3Type.Raw> arguments,
                   boolean argumentsSpecified,
                   boolean ifExists)
        {
            this.name = name;
            this.arguments = arguments;
            this.argumentsSpecified = argumentsSpecified;
            this.ifExists = ifExists;
        }

        public DropAggregateStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.keyspace : state.getKeyspace();
            return new DropAggregateStatement(keyspaceName, name.name, arguments, argumentsSpecified, ifExists);
        }
    }
}
