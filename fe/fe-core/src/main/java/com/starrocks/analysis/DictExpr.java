package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.thrift.TExprNode;
import com.starrocks.thrift.TExprNodeType;

public class DictExpr extends Expr {

    public DictExpr(Expr ref, Expr call) {
        super(ref);
        this.addChild(ref);
        this.addChild(call);
    }

    protected DictExpr(DictExpr other) {
        super(other);
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    protected String toSqlImpl() {
        return "DictExpr(" + this.getChild(0).toSqlImpl() + ",[" + this.getChild(1).toSqlImpl()  + "])";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNode_type(TExprNodeType.DICT_EXPR);
    }

    @Override
    public Expr clone() {
        return new DictExpr(this);
    }
}
