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

package org.apache.fluss.client.utils;

import org.apache.fluss.metadata.AlterTableConfigsOpType;
import org.apache.fluss.metadata.FlussTableChange;
import org.apache.fluss.rpc.messages.PbAlterConfigsRequestInfo;
import org.apache.fluss.rpc.messages.PbFlussTableChange;
import org.apache.fluss.rpc.messages.PbResetOption;
import org.apache.fluss.rpc.messages.PbSetOption;

/** Convert {@link FlussTableChange} to proto. */
public class FlussTableChangeProtoConverter {

    public static PbAlterConfigsRequestInfo toPbAlterConfigsRequestInfo(
            FlussTableChange tableChange) {
        PbAlterConfigsRequestInfo info = new PbAlterConfigsRequestInfo();
        if (tableChange instanceof FlussTableChange.SetOption) {
            FlussTableChange.SetOption setOption = (FlussTableChange.SetOption) tableChange;
            info.setConfigKey(setOption.getKey());
            info.setConfigValue(setOption.getValue());
            info.setOpType(AlterTableConfigsOpType.SET.toInt());
        } else if (tableChange instanceof FlussTableChange.ResetOption) {
            FlussTableChange.ResetOption resetOption = (FlussTableChange.ResetOption) tableChange;
            info.setConfigKey(resetOption.getKey());
            info.setOpType(AlterTableConfigsOpType.DELETE.toInt());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported table change: " + tableChange.getClass());
        }
        return info;
    }

    public static PbFlussTableChange toProto(FlussTableChange tableChange) {
        PbFlussTableChange proto = new PbFlussTableChange();
        if (tableChange instanceof FlussTableChange.SetOption) {
            PbSetOption pbSetOption = toPbSetOption((FlussTableChange.SetOption) tableChange);
            proto.setChangeType(PbFlussTableChange.ChangeType.SET_OPTION);
            proto.setSetOption(pbSetOption);
        } else if (tableChange instanceof FlussTableChange.ResetOption) {
            PbResetOption pbResetOption =
                    toPbResetOption((FlussTableChange.ResetOption) tableChange);
            proto.setChangeType(PbFlussTableChange.ChangeType.RESET_OPTION);
            proto.setResetOption(pbResetOption);
        }

        return proto;
    }

    private static PbSetOption toPbSetOption(FlussTableChange.SetOption setOption) {
        PbSetOption pbSetOption = new PbSetOption();
        pbSetOption.setKey(setOption.getKey());
        pbSetOption.setValue(setOption.getValue());
        return pbSetOption;
    }

    private static PbResetOption toPbResetOption(FlussTableChange.ResetOption resetOption) {
        PbResetOption pbResetOption = new PbResetOption();
        pbResetOption.setKey(resetOption.getKey());
        return pbResetOption;
    }
}
