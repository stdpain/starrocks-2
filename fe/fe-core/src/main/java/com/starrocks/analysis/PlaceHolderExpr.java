// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TVSlotRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PlaceHolderExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(PlaceHolderExpr.class);

    private final int slotId;
    boolean nullable;

    public PlaceHolderExpr(int slotId, boolean nullable, Type type) {
        super();
        this.slotId = slotId;
        this.nullable = nullable;
        this.type = type;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        LOG.warn("unreachable path");
        Preconditions.checkState(false);
    }

    @Override
    protected String toSqlImpl() {
        return "<slot " + slotId + ">" + nullable;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.VSLOT_REF);
        msg.setVslot_ref(new TVSlotRef());
        msg.vslot_ref.setNullable(nullable);
        msg.vslot_ref.setSlot_id(slotId);
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public Expr clone() {
        return new PlaceHolderExpr(slotId, nullable, type);
    }
}
