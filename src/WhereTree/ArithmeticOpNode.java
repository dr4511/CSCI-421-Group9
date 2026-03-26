package WhereTree;

import Catalog.TableSchema;
import Common.Record;

public class ArithmeticOpNode implements IOperandNode {
    private final IOperandNode left;
    private final IOperandNode right;
    private final String operator;

    public ArithmeticOpNode(IOperandNode left, String operator, IOperandNode right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    @Override
    public Object getValue(Record record, TableSchema table) {
        Object leftVal = left.getValue(record, table);
        Object rightVal = right.getValue(record, table);

        if (leftVal == null || rightVal == null) {
            throw new IllegalArgumentException("Cannot perform arithmetic on NULL values");
        }

        if (leftVal instanceof Integer && rightVal instanceof Integer) {
            int l = (Integer) leftVal;
            int r = (Integer) rightVal;
            switch (operator) {
                case "+": return l + r;
                case "-": return l - r;
                case "*": return l * r;
                case "/": return l / r;
                default: throw new IllegalArgumentException("Unknown operator: " + operator);
            }
        }

        if (leftVal instanceof Double && rightVal instanceof Double) {
            double l = (Double) leftVal;
            double r = (Double) rightVal;
            switch (operator) {
                case "+": return l + r;
                case "-": return l - r;
                case "*": return l * r;
                case "/": return l / r;
                default: throw new IllegalArgumentException("Unknown math operator: " + operator);
            }
        }

        throw new IllegalArgumentException(
            "Math operations require matching INTEGER or DOUBLE types, got "
            + leftVal.getClass().getSimpleName() + " and " + rightVal.getClass().getSimpleName()
        );
    }
}