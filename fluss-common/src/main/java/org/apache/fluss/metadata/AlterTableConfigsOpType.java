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

package org.apache.fluss.metadata;

/** The operation type of altering table configurations. */
public enum AlterTableConfigsOpType {
    SET(1),
    DELETE(2),
    APPEND(3),
    SUBTRACT(4);

    public final int value;

    AlterTableConfigsOpType(int value) {
        this.value = value;
    }

    public static AlterTableConfigsOpType fromInt(int opType) {
        switch (opType) {
            case 1:
                return SET;
            case 2:
                return DELETE;
            case 3:
                return APPEND;
            case 4:
                return SUBTRACT;
            default:
                throw new IllegalArgumentException(
                        "Unsupported AlterTableConfigsOpType: " + opType);
        }
    }

    public int toInt() {
        return this.value;
    }
}
