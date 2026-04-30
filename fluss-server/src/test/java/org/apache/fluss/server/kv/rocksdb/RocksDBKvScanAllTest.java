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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.config.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocksDBKv#scanAll}. */
class RocksDBKvScanAllTest {

    @Test
    void testScanAllEmpty(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());

        try (RocksDBKv rocksDBKv = rocksDBKvBuilder.build()) {
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            rocksDBKv.scanAll(
                    (k, v) -> {
                        keys.add(k);
                        values.add(v);
                    });
            assertThat(keys).isEmpty();
            assertThat(values).isEmpty();
        }
    }

    @Test
    void testScanAllWithEntries(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());

        try (RocksDBKv rocksDBKv = rocksDBKvBuilder.build()) {
            byte[] key1 = new byte[] {1, 2, 3};
            byte[] val1 = new byte[] {10, 20};
            byte[] key2 = new byte[] {4, 5, 6};
            byte[] val2 = new byte[] {30, 40, 50};

            rocksDBKv.put(key1, val1);
            rocksDBKv.put(key2, val2);

            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            rocksDBKv.scanAll(
                    (k, v) -> {
                        keys.add(k);
                        values.add(v);
                    });

            assertThat(keys).hasSize(2);
            assertThat(values).hasSize(2);

            // RocksDB iterates in key order, so key1 < key2
            assertThat(keys.get(0)).containsExactly(1, 2, 3);
            assertThat(values.get(0)).containsExactly(10, 20);
            assertThat(keys.get(1)).containsExactly(4, 5, 6);
            assertThat(values.get(1)).containsExactly(30, 40, 50);
        }
    }

    @Test
    void testScanAllReturnsCorrectKeyValuePairs(@TempDir Path tempDir) throws Exception {
        File instanceBasePath = tempDir.toFile();
        RocksDBResourceContainer rocksDBResourceContainer =
                new RocksDBResourceContainer(new Configuration(), instanceBasePath);
        RocksDBKvBuilder rocksDBKvBuilder =
                new RocksDBKvBuilder(
                        instanceBasePath,
                        rocksDBResourceContainer,
                        rocksDBResourceContainer.getColumnOptions());

        try (RocksDBKv rocksDBKv = rocksDBKvBuilder.build()) {
            // Put, overwrite, then scan
            byte[] key = new byte[] {1};
            rocksDBKv.put(key, new byte[] {10});
            rocksDBKv.put(key, new byte[] {20});

            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            rocksDBKv.scanAll(
                    (k, v) -> {
                        keys.add(k);
                        values.add(v);
                    });

            assertThat(keys).hasSize(1);
            assertThat(keys.get(0)).containsExactly(1);
            assertThat(values.get(0)).containsExactly(20);
        }
    }
}
