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

package com.alibaba.fluss.record;

import com.alibaba.fluss.annotation.Internal;

import java.io.IOException;

/**
 * An abstraction between an underlying input stream and record iterators.
 *
 * <p>The generic typing allows for implementations which present only a view of the log entries,
 * which enables more efficient iteration when the record data is not actually needed. See for
 * example {@link FileLogInputStream.FileChannelLogRecordBatch} in which the record is not brought
 * into memory until needed.
 *
 * @param <T> Type parameter of the log entry
 */
@Internal
public interface LogInputStream<T extends LogRecordBatch> {

    /**
     * Get the next record batch from the underlying input stream.
     *
     * @return The next record batch or null if there is none
     * @throws IOException for any IO errors
     */
    T nextBatch() throws IOException;
}
