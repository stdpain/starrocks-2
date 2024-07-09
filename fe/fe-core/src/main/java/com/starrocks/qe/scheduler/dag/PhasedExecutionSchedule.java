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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.UserException;
import com.starrocks.common.util.UnionFind;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.rpc.RpcException;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.thrift.TUniqueId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PhasedExecutionSchedule implements ExecutionSchedule {
    private static class PackedExecutionFragment {
        PackedExecutionFragment(ExecutionFragment fragment) {
            fragments.add(fragment);
        }

        PackedExecutionFragment(Collection<ExecutionFragment> fragments) {
            this.fragments.addAll(fragments);
        }

        private final List<ExecutionFragment> fragments = Lists.newArrayList();

        List<ExecutionFragment> getFragments() {
            return fragments;
        }
    }

    private Deployer deployer;
    private ExecutionDAG dag;

    public PhasedExecutionSchedule(ConnectContext context) {
        this.connectContext = context;
        this.maxScheduleConcurrency = context.getSessionVariable().getPhasedSchedulerMaxConcurrency();
    }

    private final Stack<PackedExecutionFragment> stack = new Stack<>();

    private int currentScheduleConcurrency = 0;
    private final int maxScheduleConcurrency;
    //    private Stack<ExecutionFragment> stack = new Stack<>();
    // fragment id -> in-degree > 1 children (cte producer fragments)
    private final Map<PlanFragmentId, Set<PlanFragmentId>> commonChildrens = Maps.newHashMap();
    private Map<PlanFragmentId, Integer> fragmentInDegrees = Maps.newHashMap();
    private final UnionFind<PlanFragmentId> mergedUnionFind = new UnionFind<>();
    // Controls that only one thread can call scheduleNext.
    private final AtomicInteger inputScheduleTaskNums = new AtomicInteger();
    private final ConnectContext connectContext;

    // TODO: coordinate in BE
    private Map<PlanFragmentId, AtomicInteger> schedulingFragmentInstances = Maps.newConcurrentMap();

    public void prepareSchedule(Deployer deployer, ExecutionDAG dag) {
        this.deployer = deployer;
        this.dag = dag;
        ExecutionFragment rootFragment = dag.getRootFragment();
        this.stack.push(new PackedExecutionFragment(rootFragment));
        // build in-degrees
        fragmentInDegrees = buildChildInDegrees(rootFragment);
        // build common children
        buildCommonChildren(rootFragment);

        buildMergedSets(rootFragment, Sets.newHashSet());
    }

    private Map<PlanFragmentId, Integer> buildChildInDegrees(ExecutionFragment root) {
        Queue<ExecutionFragment> queue = Lists.newLinkedList();
        Map<PlanFragmentId, Integer> inDegrees = Maps.newHashMap();
        inDegrees.put(root.getFragmentId(), 0);
        queue.add(root);

        while (!queue.isEmpty()) {
            final ExecutionFragment fragment = queue.poll();
            for (int i = 0; i < fragment.childrenSize(); i++) {
                ExecutionFragment child = fragment.getChild(i);
                PlanFragmentId cid = fragment.getChild(i).getFragmentId();
                Integer v = inDegrees.get(cid);
                if (v != null) {
                    inDegrees.put(cid, v + 1);
                } else {
                    inDegrees.put(cid, 1);
                    queue.add(child);
                }
            }
        }

        return inDegrees;
    }

    private Set<PlanFragmentId> buildCommonChildren(ExecutionFragment fragment) {
        final PlanFragmentId fragmentId = fragment.getFragmentId();
        final Set<PlanFragmentId> planFragmentIds = commonChildrens.get(fragmentId);

        if (planFragmentIds != null) {
            return planFragmentIds;
        }

        final Set<PlanFragmentId> fragmentCommonChild = Sets.newHashSet();

        if (fragmentInDegrees.get(fragmentId) > 1) {
            fragmentCommonChild.add(fragmentId);
        }

        for (int i = 0; i < fragment.childrenSize(); i++) {
            final ExecutionFragment child = fragment.getChild(i);
            final Set<PlanFragmentId> childCommonChildren = buildCommonChildren(child);
            fragmentCommonChild.addAll(childCommonChildren);
        }

        commonChildrens.put(fragmentId, fragmentCommonChild);
        return fragmentCommonChild;
    }

    private void buildMergedSets(ExecutionFragment node, Set<PlanFragmentId> accessed) {
        final PlanFragmentId fragmentId = node.getFragmentId();

        if (accessed.contains(fragmentId)) {
            return;
        }
        accessed.add(fragmentId);

        final Set<PlanFragmentId> planFragmentIds = commonChildrens.get(fragmentId);
        for (PlanFragmentId planFragmentId : planFragmentIds) {
            mergedUnionFind.union(fragmentId, planFragmentId);
        }
        for (int i = 0; i < node.childrenSize(); i++) {
            final ExecutionFragment child = node.getChild(i);
            buildMergedSets(child, accessed);
        }
    }

    // schedule next
    public Collection<FragmentInstanceExecState> schedule() throws RpcException, UserException {
        final List<FragmentInstanceExecState> executions = Lists.newArrayList();
        final int oldTaskCnt = inputScheduleTaskNums.getAndIncrement();
        if (oldTaskCnt == 0) {
            int dec = 0;
            do {
                final Collection<FragmentInstanceExecState> scheduledExecutions = scheduleNextTurn();
                executions.addAll(scheduledExecutions);
                dec = inputScheduleTaskNums.getAndDecrement();
            } while (dec > 1);
        }
        return executions;
    }

    public void tryScheduleNextTurn(CriticalAreaRunner criticalRunner, TUniqueId fragmentInstanceId)
            throws RpcException, UserException {
        final FragmentInstance instance = dag.getInstanceByInstanceId(fragmentInstanceId);
        final PlanFragmentId fragmentId = instance.getFragmentId();
        final AtomicInteger countDowns = schedulingFragmentInstances.get(fragmentId);
        if (countDowns.decrementAndGet() != 0) {
            return;
        }
        currentScheduleConcurrency--;
        try (var guard = ConnectContext.ScopeGuard.setIfNotExists(connectContext)) {
            final int oldTaskCnt = inputScheduleTaskNums.getAndIncrement();
            if (oldTaskCnt == 0) {
                int dec = 0;
                do {
                    criticalRunner.accept(this::scheduleNextTurn);
                    dec = inputScheduleTaskNums.getAndDecrement();
                } while (dec > 1);
            }
        }
    }

    // inner data structure (ExecutionDag::executors) should be protected by Coordinator lock
    private Collection<FragmentInstanceExecState> scheduleNextTurn() throws RpcException, UserException {
        if (isFinished()) {
            return Collections.emptyList();
        }
        List<List<ExecutionFragment>> scheduleFragments = Lists.newArrayList();
        while (currentScheduleConcurrency < maxScheduleConcurrency) {
            if (isFinished()) {
                break;
            }
            if (scheduleNext(scheduleFragments)) {
                currentScheduleConcurrency++;
            }
        }
        // merge and build fragment
        final List<List<ExecutionFragment>> fragments = buildScheduleOrder(scheduleFragments);
        // deploy fragments
        for (List<ExecutionFragment> fragment : fragments) {
            for (ExecutionFragment executionFragment : fragment) {
                final PlanFragmentId fragmentId = executionFragment.getFragmentId();
                int instanceNums = executionFragment.getInstances().size();
                schedulingFragmentInstances.put(fragmentId, new AtomicInteger(instanceNums));
            }
            deployer.deployFragments(fragment);
        }

        // collect executions
        final List<FragmentInstanceExecState> executions = Lists.newArrayList();
        for (List<ExecutionFragment> fragment : fragments) {
            for (ExecutionFragment executionFragment : fragment) {
                for (FragmentInstance instance : executionFragment.getInstances()) {
                    final FragmentInstanceExecState execution = instance.getExecution();
                    executions.add(execution);
                }
            }
        }

        return executions;
    }

    private boolean scheduleNext(List<List<ExecutionFragment>> scheduleFragments) {
        List<ExecutionFragment> currentScheduleFragments = Lists.newArrayList();
        PackedExecutionFragment packedExecutionFragment = stack.pop();

        currentScheduleFragments.addAll(packedExecutionFragment.getFragments());
        for (ExecutionFragment fragment : currentScheduleFragments) {
            if (fragment.isScheduled()) {
                continue;
            }

            final Map<Integer, Set<ExecutionFragment>> groups = Maps.newHashMap();
            final Set<PlanFragmentId> processedFragments = Sets.newHashSet();

            for (int i = 0; i < fragment.childrenSize(); i++) {
                final ExecutionFragment child = fragment.getChild(i);
                final PlanFragmentId childFragmentId = child.getFragmentId();
                final int groupId = mergedUnionFind.getGroupIdOrAdd(childFragmentId);
                Set<ExecutionFragment> groupFragments = groups.computeIfAbsent(groupId, k -> Sets.newHashSet());
                groupFragments.add(child);
            }

            if (groups.size() != fragment.childrenSize()) {
                for (int i = 0; i < fragment.childrenSize(); i++) {
                    final ExecutionFragment child = fragment.getChild(i);
                    final PlanFragmentId childFragmentId = child.getFragmentId();
                    final int groupId = mergedUnionFind.getGroupIdOrAdd(childFragmentId);
                    final Set<ExecutionFragment> groupFragments = groups.get(groupId);
                    // The current fragment is in the same group as the prev fragment and
                    // does not need to be processed.
                    if (processedFragments.contains(childFragmentId)) {
                        continue;
                    }

                    stack.push(new PackedExecutionFragment(groupFragments));

                    for (ExecutionFragment groupFragment : groupFragments) {
                        processedFragments.add(groupFragment.getFragmentId());
                    }
                    if (i != 0) {
                        child.setNeedReportFragmentFinish(true);
                    }
                }
            } else {
                for (int i = 0; i < fragment.childrenSize(); i++) {
                    final ExecutionFragment child = fragment.getChild(i);
                    if (i != 0) {
                        child.setNeedReportFragmentFinish(true);
                    }
                    stack.push(new PackedExecutionFragment(child));
                }
            }
        }

        if (!currentScheduleFragments.isEmpty()) {
            scheduleFragments.add(currentScheduleFragments);
            for (ExecutionFragment currentScheduleFragment : currentScheduleFragments) {
                if (currentScheduleFragment.childrenSize() == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    private List<List<ExecutionFragment>> buildScheduleOrder(List<List<ExecutionFragment>> scheduleFragments) {
        List<List<ExecutionFragment>> groups = Lists.newArrayList();

        final ExecutionFragment captureVersionFragment = dag.getCaptureVersionFragment();
        if (captureVersionFragment != null && !captureVersionFragment.isScheduled()) {
            groups.add(Lists.newArrayList(captureVersionFragment));
            captureVersionFragment.setIsScheduled(true);
        }

        final Set<ExecutionFragment> fragments =
                scheduleFragments.stream().flatMap(Collection::stream).collect(Collectors.toUnmodifiableSet());

        // collect zero in-degree nodes
        Queue<ExecutionFragment> queue = Lists.newLinkedList();
        for (ExecutionFragment fragment : fragments) {
            if (fragmentInDegrees.get(fragment.getFragmentId()) == 0) {
                queue.add(fragment);
            }
        }

        // top-sort for input fragments
        int scheduleFragmentNums = 0;
        while (!queue.isEmpty()) {
            int groupSize = queue.size();
            List<ExecutionFragment> group = new ArrayList<>(groupSize);
            // The next `groupSize` fragments can be delivered concurrently, because zero in-degree indicates that
            // they don't depend on each other and all the fragments depending on them have been delivered.
            for (int i = 0; i < groupSize; ++i) {
                ExecutionFragment fragment = Preconditions.checkNotNull(queue.poll());
                fragment.setIsScheduled(true);
                group.add(fragment);

                for (int j = 0; j < fragment.childrenSize(); j++) {
                    ExecutionFragment child = fragment.getChild(j);
                    final PlanFragmentId fragmentId = child.getFragmentId();
                    int degree = fragmentInDegrees.compute(fragmentId, (k, v) -> Preconditions.checkNotNull(v) - 1);
                    if (degree == 0 && fragments.contains(child)) {
                        queue.add(child);
                    }
                }
            }
            groups.add(group);
            scheduleFragmentNums += group.size();
        }
        if (scheduleFragmentNums != fragments.size()) {
            throw new StarRocksPlannerException("invalid schedule plan",
                    ErrorType.INTERNAL_ERROR);
        }

        return groups;
    }

    public boolean isFinished() {
        return stack.isEmpty();
    }

}