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

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.UserException;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TUniqueId;

import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

public class PhasedExecutionSchedule implements ExecutionSchedule {
    private Deployer deployer;
    private ExecutionDAG dag;
    private Stack<ExecutionFragment> stack = new Stack<>();
    private Map<PlanFragmentId, AtomicInteger> schedulingFragmentInstances = Maps.newConcurrentMap();
    private AtomicInteger inputScheduleTaskNums = new AtomicInteger();
    private ConnectContext connectContext;

    public PhasedExecutionSchedule(ConnectContext context) {
        this.connectContext = context;
    }

    public void prepareSchedule(Deployer deployer, ExecutionDAG dag) {
        this.deployer = deployer;
        this.dag = dag;
        ExecutionFragment rootFragment = dag.getRootFragment();
        stack.push(rootFragment);
    }

    // schedule next
    public void schedule() throws RpcException, UserException {
        // finish

        if (isFinished()) {
            return;
        }

        final int oldTaskCnt = inputScheduleTaskNums.getAndIncrement();
        if (oldTaskCnt == 0) {
            int dec = 0;
            do {
                while (!scheduleNext()) {}
                dec = inputScheduleTaskNums.getAndDecrement();
            } while (dec == 1);
        }
    }

    public void tryScheduleNextTurn(TUniqueId fragmentInstanceId) throws RpcException, UserException {
        final FragmentInstance instance = dag.getInstanceByInstanceId(fragmentInstanceId);
        final PlanFragmentId fragmentId = instance.getFragmentId();
        final AtomicInteger countDowns = schedulingFragmentInstances.get(fragmentId);
        if (countDowns.decrementAndGet() == 0) {
            try (var guard = ConnectContext.ScopeGuard.setIfNotExists(connectContext)) {
                schedule();
            }
        }
    }

    private boolean scheduleNext() throws RpcException, UserException {
        ExecutionFragment fragment = stack.pop();
        for (int i = 0; i < fragment.childrenSize(); i++) {
            stack.push(fragment.getChild(i));
        }

        List<ExecutionFragment> deploy = Lists.newArrayList();
        deploy.add(fragment);
        schedulingFragmentInstances.put(fragment.getFragmentId(), new AtomicInteger(fragment.getInstances().size()));
        deployer.deployFragments(deploy);

        return fragment.childrenSize() == 0;
    }

    public boolean isFinished() {
        return stack.empty();
    }
}