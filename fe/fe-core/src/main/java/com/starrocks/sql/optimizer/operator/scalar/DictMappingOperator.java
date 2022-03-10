// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DictMappingOperator extends ScalarOperator {

    private final ColumnRefOperator argument;
    private final ScalarOperator innerOperator;

    public DictMappingOperator(ColumnRefOperator input, ScalarOperator innerOperator, Type retType) {
        super(OperatorType.DICT_MAPPING, retType);
        this.argument = input;
        this.innerOperator = innerOperator;
    }

    public ColumnRefOperator getArgument() {
        return argument;
    }

    public ScalarOperator getInnerOperator() {
        return innerOperator;
    }

    @Override
    public boolean isNullable() {
        return innerOperator.isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public ScalarOperator getChild(int index) {
        return null;
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
    }

    @Override
    public String toString() {
        return "DictMapping(" + argument + "{" + innerOperator + "}" + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(argument, innerOperator);
    }

    @Override
    public boolean equals(Object other) {
        return false;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitDictMappingOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return argument.getUsedColumns();
    }
}
