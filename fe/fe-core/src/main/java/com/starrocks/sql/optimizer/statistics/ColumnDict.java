// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;

import java.nio.ByteBuffer;

public final class ColumnDict {
    private final ImmutableMap<ByteBuffer, Integer> dict;
    private final long collectedVersionTime;
    private long versionTime;

    public ColumnDict(ImmutableMap<ByteBuffer, Integer> dict, long versionTime) {
        // TODO: The default value of low_cardinality_threshold is 255. Should we set the check size to 255 or 256?
        Preconditions.checkState(!dict.isEmpty() && dict.size() <= Config.low_cardinality_threshold + 1,
                "dict size %s is illegal", dict.size());
        this.dict = dict;
        this.collectedVersionTime = versionTime;
        this.versionTime = versionTime;
    }

    public ImmutableMap<ByteBuffer, Integer> getDict() {
        return dict;
    }

    public long getVersionTime() {
        return versionTime;
    }

    public long getCollectedVersionTime() {
        return collectedVersionTime;
    }

    void updateVersionTime(long versionTime) {
        this.versionTime = versionTime;
    }
}