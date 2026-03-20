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

package org.apache.fluss.row;

import org.apache.fluss.annotation.Internal;

import java.util.Arrays;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * Wraps a partial row (containing only a subset of columns) to present a full schema view. Returns
 * null for non-target columns.
 *
 * <p>This is used by the partial insert feature for log tables. When a writer inserts only a subset
 * of columns, the row data is stored with the projected schema. During reading, this wrapper maps
 * the projected positions back to the full schema positions.
 */
@Internal
public class PartialRow implements InternalRow {

    private final InternalRow delegate;
    private final int[] targetColumns;
    private final int[] positionMap;
    private final int totalFieldCount;

    /**
     * Creates a new PartialRow wrapper.
     *
     * @param delegate the underlying partial row containing only target columns data
     * @param targetColumns the original column indices that the delegate row contains
     * @param totalFieldCount the total number of fields in the full schema
     */
    public PartialRow(InternalRow delegate, int[] targetColumns, int totalFieldCount) {
        this.delegate = delegate;
        this.targetColumns = targetColumns;
        this.totalFieldCount = totalFieldCount;
        this.positionMap = buildPositionMap();
    }

    private int[] buildPositionMap() {
        int[] map = new int[totalFieldCount];
        Arrays.fill(map, -1);
        for (int i = 0; i < targetColumns.length; i++) {
            map[targetColumns[i]] = i;
        }
        return map;
    }

    @Override
    public int getFieldCount() {
        return totalFieldCount;
    }

    @Override
    public boolean isNullAt(int pos) {
        int delegatePos = positionMap[pos];
        if (delegatePos < 0) {
            return true; // Column not present in partial row
        }
        return delegate.isNullAt(delegatePos);
    }

    @Override
    public boolean getBoolean(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getBoolean(delegatePos);
    }

    @Override
    public byte getByte(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getByte(delegatePos);
    }

    @Override
    public short getShort(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getShort(delegatePos);
    }

    @Override
    public int getInt(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getInt(delegatePos);
    }

    @Override
    public long getLong(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getLong(delegatePos);
    }

    @Override
    public float getFloat(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getFloat(delegatePos);
    }

    @Override
    public double getDouble(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getDouble(delegatePos);
    }

    @Override
    public BinaryString getChar(int pos, int length) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getChar(delegatePos, length);
    }

    @Override
    public BinaryString getString(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getString(delegatePos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getDecimal(delegatePos, precision, scale);
    }

    @Override
    public TimestampNtz getTimestampNtz(int pos, int precision) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getTimestampNtz(delegatePos, precision);
    }

    @Override
    public TimestampLtz getTimestampLtz(int pos, int precision) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getTimestampLtz(delegatePos, precision);
    }

    @Override
    public byte[] getBinary(int pos, int length) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getBinary(delegatePos, length);
    }

    @Override
    public byte[] getBytes(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getBytes(delegatePos);
    }

    @Override
    public InternalArray getArray(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getArray(delegatePos);
    }

    @Override
    public InternalMap getMap(int pos) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getMap(delegatePos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        int delegatePos = getDelegatePos(pos);
        return delegate.getRow(delegatePos, numFields);
    }

    private int getDelegatePos(int pos) {
        int delegatePos = positionMap[pos];
        checkArgument(delegatePos >= 0, "Column %d is not present in partial row", pos);
        return delegatePos;
    }
}
