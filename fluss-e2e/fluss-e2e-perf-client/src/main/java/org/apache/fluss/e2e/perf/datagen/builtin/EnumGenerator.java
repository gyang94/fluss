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

package org.apache.fluss.e2e.perf.datagen.builtin;

import org.apache.fluss.e2e.perf.datagen.FieldGenerator;

import java.util.List;
import java.util.Map;
import java.util.Random;

/** Selects a random value from a configured list of strings. */
public class EnumGenerator implements FieldGenerator {

    private String[] values;
    private Random rng;

    @Override
    public String type() {
        return "enum";
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, Object> params, long seed) {
        List<String> list = (List<String>) params.get("values");
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException(
                    "EnumGenerator requires non-empty 'values' parameter");
        }
        this.values = list.toArray(new String[0]);
        this.rng = new Random(seed);
    }

    @Override
    public Object generate(long index) {
        return values[rng.nextInt(values.length)];
    }
}
