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

/**
 * Basic implementation of {@link SchemaTransformation.Result}.
 *
 * @param <S> the subtype of {@link Schema} used by the result. This is not meant to be expose outside this package
 * since only the top-level {@link Schema} interface is public, but not its sub-classes. However, for individual
 * {@link SchemaUpdateHandler} implementation, it's useful to preserve the exact type of their schema for their
 * internal implementation.
 */
class SchemaTransformationResult<S extends Schema> implements SchemaTransformation.Result
{
    final S before;
    final S after;
    final KeyspacesDiff diff;

    SchemaTransformationResult(S before, S after, KeyspacesDiff diff)
    {
        this.before = before;
        this.after = after;
        this.diff = diff;
    }

    @Override
    public Schema schema(TransformationSide side)
    {
        return side == TransformationSide.BEFORE ? before : after;
    }

    @Override
    public KeyspacesDiff diff()
    {
        return diff;
    }

    @Override
    public String toString()
    {
        return String.format("%s -> %s (diff: %s)", before, after, diff);
    }
}