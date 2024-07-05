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

package com.starrocks.qe.scheduler.dag;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.planner.BlackHoleTableSink;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TScanRangeParams;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CaptureVersionFragmentBuilder {
    private final List<ExecutionFragment> fragments;

    public CaptureVersionFragmentBuilder(List<ExecutionFragment> fragments) {
        this.fragments = fragments;
    }

    public ExecutionFragment build(ExecutionDAG dag) {
        Map<ComputeNode, List<TScanRangeParams>> workerId2ScanRanges = Maps.newHashMap();
        int id = 0;
        for (ExecutionFragment fragment : fragments) {
            final PlanFragmentId fragmentId = fragment.getFragmentId();
            for (FragmentInstance instance : fragment.getInstances()) {
                final ComputeNode worker = instance.getWorker();
                final Map<Integer, List<TScanRangeParams>> node2ScanRanges = instance.getNode2ScanRanges();
                // collect olap scan ranges
                final List<TScanRangeParams> instanceScanRanges =
                        node2ScanRanges.values().stream().flatMap(Collection::stream).filter(
                                TScanRangeParams::isSetScan_range).collect(Collectors.toList());

                if (instanceScanRanges.isEmpty()) {
                    continue;
                }

                final List<TScanRangeParams> workerIdScanRanges =
                        workerId2ScanRanges.computeIfAbsent(worker, k -> Lists.newArrayList());
                workerIdScanRanges.addAll(instanceScanRanges);
            }
            id = Math.max(id, fragmentId.asInt());
        }

        if (!workerId2ScanRanges.isEmpty()) {
            final PlanFragmentId captureVersionFragmentId = new PlanFragmentId(id + 1);
            final int fid = captureVersionFragmentId.asInt();
            PlanNode dummyNode = new EmptySetNode(PlanNodeId.DUMMY_PLAN_NODE_ID, Lists.newArrayList());
            final PlanFragment captureVersionFragment =
                    new PlanFragment(captureVersionFragmentId, dummyNode, DataPartition.RANDOM);
            captureVersionFragment.setSink(new BlackHoleTableSink());
            final ExecutionFragment fragment = new ExecutionFragment(dag, captureVersionFragment, fid);

            workerId2ScanRanges.forEach((worker, scanRanges) -> {
                final FragmentInstance instance = new FragmentInstance(worker, fragment);
                instance.addScanRanges(PlanNodeId.DUMMY_PLAN_NODE_ID.asInt(), scanRanges);
                fragment.addInstance(instance);
            });

            return fragment;
        }

        return null;
    }
}
