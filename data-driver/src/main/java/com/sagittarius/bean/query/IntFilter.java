package com.sagittarius.bean.query;

/**
 * this filter is implemented as a binary tree, only leaf filter contains filterType and the key value
 */
public class IntFilter {
    private NumericFilterType filterType;
    private int key;

    private IntFilter left;
    private IntFilter right;
    private LogicType logicType;

    /**
     * constructor for leaf filter
     */
    public IntFilter(NumericFilterType filterType, int key) {
        this.filterType = filterType;
        this.key = key;
    }

    /**
     * constructor for none-leaf filter. The left filter and right filter can be set to null.
     * Null filters always mean true and you must be careful of this.
     */
    public IntFilter(IntFilter left, IntFilter right, LogicType logicType) {
        this.left = left;
        this.right = right;
        this.logicType = logicType;
    }

    public boolean filter(int dataValue){
        if (filterType != null) { //leaf filter
            switch (filterType) {
                case EQ:
                    return dataValue == key;
                case NEQ:
                    return dataValue != key;
                case GT:
                    return dataValue > key;
                case LT:
                    return dataValue < key;
                case GTE:
                    return dataValue >= key;
                case LTE:
                    return dataValue <= key;
            }
        } else { //none-leaf filter
            boolean leftResult = true, rightResult = true;
            if (left != null) {
                leftResult = left.filter(dataValue);
            }
            if (right != null) {
                rightResult = right.filter(dataValue);
            }

            switch (logicType) {
                case AND:
                    return leftResult && rightResult;
                case OR:
                    return leftResult || rightResult;
            }
        }

        return true;
    }
}
