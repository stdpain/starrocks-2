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

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializeNativeTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("CREATE TABLE supplier_nullable ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             S_NAME        CHAR(25) NOT NULL,\n" +
                "                             S_ADDRESS     VARCHAR(40), \n" +
                "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             S_PHONE       CHAR(15) NOT NULL,\n" +
                "                             S_ACCTBAL     double NOT NULL,\n" +
                "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 4\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @Test
    public void testLazyMaterialize() throws Exception {
        String sql;
        String plan;
        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "  6:Decode\n" +
                "  |  <dict id 10> : <string id 3>\n" +
                "  |  <dict id 11> : <string id 7>\n" +
                "  |  <dict id 12> : <string id 9>");

        sql = "select distinct S_SUPPKEY, S_ADDRESS from ( select S_ADDRESS, S_SUPPKEY " +
                "from supplier_nullable limit 10) t";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "  6:Decode\n" +
                "  |  <dict id 9> : <string id 3>");

        sql = "select * from supplier where S_SUPPKEY < 10 order by 1,2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testProjection() throws Exception {
        String sql;
        String plan;

        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
        assertContains(plan, "Decode");
        assertContains(plan, "Project");
    }

    @Test
    public void testArraySubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select array_length(test_a1) from test_array limit 1";
        plan = getCostExplain(sql);
        assertNotContains(plan, "FETCH");

        sql = "select array_length(test_a1),* from test_array limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  4:FETCH
                  |  lookup node: 03
                  |  table: test_array
                  |    <slot 1> => test_key
                  |    <slot 3> => PAD
                  |  limit: 1\
                """);
    }

    @Test
    public void testStructSubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select test_struct.name from test_struct limit 1";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    @Test
    public void testLeftJoin() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct l join test_array r on l.test_key=r.test_key limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testWithOffset() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct order by 1 limit 100000000,10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }
}
