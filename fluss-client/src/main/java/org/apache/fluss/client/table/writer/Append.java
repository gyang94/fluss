/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Used to configure and create a {@link AppendWriter} to write data to a Log Table.
 *
 * <p>{@link Append} objects are immutable and can be shared between threads. Refinement methods,
 * like {@link #partialInsert(int[])}, create new Append instances.
 *
 * @since 0.6
 */
@PublicEvolving
public interface Append {

    /**
     * Apply partial insert columns and returns a new Append instance.
     *
     * <p>For append operations, only the specified columns will be written. Non-target columns will
     * be treated as null when reading.
     *
     * <p>Note: If the table has partition keys or bucket keys, the specified columns must contain
     * all of them. All non-target columns must be nullable.
     *
     * @param targetColumns the column indexes to partial insert; must not be null or empty
     * @return a new Append instance with partial insert configured
     */
    Append partialInsert(int[] targetColumns);

    /**
     * @see #partialInsert(int[]) for more details.
     * @param targetColumnNames the column names to partial insert
     * @return a new Append instance with partial insert configured
     */
    Append partialInsert(String... targetColumnNames);

    /** Create a new {@link AppendWriter} to write data to a Log Table using InternalRow. */
    AppendWriter createWriter();

    /** Create a new typed {@link AppendWriter} to write POJOs directly. */
    <T> TypedAppendWriter<T> createTypedWriter(Class<T> pojoClass);
}
