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

public interface SchemaTransformation
{
    /**
     * Apply a statement transformation to a schema snapshot.
     *
     * Implementing methods should be side-effect free.
     *
     * @param schema Keyspaces to base the transformation on
     * @return Keyspaces transformed by the statement
     */
    Keyspaces apply(Keyspaces schema);

    /**
     * The result of applying (on this node) a given schema transformation.
     */
    interface Result
    {
        /**
         * Obtain the schema before and after the transformation this is the result of.
         *
         * @param side the side to return, before or after the transformation.
         * @return the schema corresponding to {@code side}.
         */
        Schema schema(TransformationSide side);

        /**
         * The diff of the changes performed.
         *
         * @return the {@link KeyspacesDiff} corresponding to the transformation this is the result of.
         */
        KeyspacesDiff diff();

        /**
         * Whether the transformation this is the result of was empty, that is did nothing (typical of DDL with a
         * condition that does not applySchemaMigration for instance).
         */
        default boolean isEmpty()
        {
            return diff().isEmpty();
        }
    }
}
