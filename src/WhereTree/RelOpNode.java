package WhereTree;

import Catalog.TableSchema;
import Common.Record;

/**
 * Represents a relational operation
 */
public class RelOpNode implements IWhereTree {
    private IOperandNode left;
    private IOperandNode right;
    private String operator;

    public RelOpNode(IOperandNode left, String operator, IOperandNode right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    @Override
    public boolean evaluate(Record record, TableSchema schema) {
        if (operator.equals("IS NULL")) {
            Object leftVal = left.getValue(record, schema);
            return leftVal == null;
        }

        Object leftVal = left.getValue(record, schema);
        Object rightVal = right.getValue(record, schema);

        if (leftVal == null || rightVal == null) {
            return false;
        }
        Comparable<Object> leftComp = (Comparable<Object>) leftVal;
        int compare = leftComp.compareTo(rightVal);
        switch (operator) {
            case ">":  return compare > 0;
            case ">=": return compare >= 0;
            case "<=": return compare <= 0;
            case "<":  return compare < 0;
            case "<>": return compare != 0;
            case "=": return compare == 0;
            default:
                throw new IllegalArgumentException("Unknown operator: " + operator);
        }
    }
}