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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.record.TestData.DATA2_TABLE_INFO;
import static org.apache.fluss.record.TestData.PARTITION_TABLE_INFO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableAppend} and {@link TypedAppendWriterImpl}. */
class TableAppendTest {

    @Test
    void testPartialInsertRequiresPartitionKeys() {
        TableAppend append =
                new TableAppend(PARTITION_TABLE_INFO.getTablePath(), PARTITION_TABLE_INFO, null);

        assertThatThrownBy(() -> append.partialInsert(new int[] {0}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Target columns must include partition key column 'b'");
    }

    @Test
    void testPartialInsertRequiresBucketKeys() {
        TableInfo bucketedTableInfo =
                TableInfo.of(
                        TablePath.of("test_db_1", "bucketed_log_table"),
                        150101L,
                        1,
                        TableDescriptor.builder()
                                .schema(DATA1_SCHEMA)
                                .distributedBy(3, "a")
                                .build(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableAppend append =
                new TableAppend(bucketedTableInfo.getTablePath(), bucketedTableInfo, null);

        assertThatThrownBy(() -> append.partialInsert(new int[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Target columns must include bucket key column 'a'");
    }

    @Test
    void testPartialInsertRejectsNonNullableExcludedColumns() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("payload", DataTypes.STRING())
                        .build();
        TableInfo tableInfo =
                TableInfo.of(
                        TablePath.of("test_db_1", "non_nullable_log_table"),
                        150102L,
                        1,
                        TableDescriptor.builder().schema(schema).distributedBy(1).build(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
        TableAppend append = new TableAppend(tableInfo.getTablePath(), tableInfo, null);

        assertThatThrownBy(() -> append.partialInsert(new int[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Non-target column 'id' is NOT NULL");
    }

    @Test
    void testTypedAppendWriterProjectsPojoForPartialInsert() {
        AtomicReference<InternalRow> captured = new AtomicReference<>();
        AppendWriter delegate =
                new AppendWriter() {
                    @Override
                    public void flush() {}

                    @Override
                    public CompletableFuture<AppendResult> append(InternalRow record) {
                        captured.set(record);
                        return CompletableFuture.completedFuture(new AppendResult());
                    }
                };

        TypedAppendWriterImpl<TestPojo> writer =
                new TypedAppendWriterImpl<>(
                        delegate, TestPojo.class, DATA2_TABLE_INFO, new int[] {0, 2});

        writer.append(new TestPojo(1, "ignored", "payload")).join();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().getFieldCount()).isEqualTo(2);
        assertThat(captured.get().getInt(0)).isEqualTo(1);
        assertThat(captured.get().getString(1).toString()).isEqualTo("payload");
    }

    public static final class TestPojo {
        public Integer a;
        public String b;
        public String c;

        public TestPojo() {}

        public TestPojo(Integer a, String b, String c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }
    }
}
