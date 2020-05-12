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
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspacesDiff;
import org.apache.cassandra.schema.TransformationSide;
import org.apache.cassandra.transport.Event;

import static java.util.stream.Collectors.toList;

abstract class AbstractCreateFunctionStatement extends AbstractFunctionStatement
{
    protected final boolean orReplace;
    protected final boolean ifNotExists;

    AbstractCreateFunctionStatement(String keyspaceName,
                                    String functionName,
                                    List<CQL3Type.Raw> rawArgumentTypes,
                                    boolean orReplace,
                                    boolean ifNotExists)
    {
        super(keyspaceName, functionName, rawArgumentTypes);
        this.orReplace = orReplace;
        this.ifNotExists = ifNotExists;
    }

    @Override
    protected void validateForApplication()
    {
        super.validateForApplication();

        if (ifNotExists && orReplace)
            throw ire("Cannot use both 'OR REPLACE' and 'IF NOT EXISTS' directives");
    }

    private boolean wasCreated(KeyspacesDiff diff)
    {
        KeyspaceMetadata keyspaceBefore = diff.keyspace(TransformationSide.BEFORE, keyspaceName);
        List<AbstractType<?>> argTypes = argumentsTypes(keyspaceBefore);
        Function before = find(keyspaceBefore, argTypes);
        Function after = find(diff.keyspace(TransformationSide.AFTER, keyspaceName), argTypes);
        return before == null && after != null;
    }

    @Override
    protected Set<IResource> createdResources(KeyspacesDiff diff)
    {
        return wasCreated(diff)
               ? ImmutableSet.of(FunctionResource.functionFromCql(keyspaceName, functionName.name, rawArgumentTypes))
               : ImmutableSet.of();
    }

    @Override
    protected Event.SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new Event.SchemaChange(wasCreated(diff) ? Event.SchemaChange.Change.CREATED : Event.SchemaChange.Change.UPDATED,
                                      schemaChangeTarget(),
                                      keyspaceName,
                                      functionName.name,
                                      rawArgumentTypes.stream().map(CQL3Type.Raw::toString).collect(toList()));
    }

    abstract protected Event.SchemaChange.Target schemaChangeTarget();
}
