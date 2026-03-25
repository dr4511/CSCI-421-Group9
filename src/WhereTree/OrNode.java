package WhereTree;

import Catalog.TableSchema;
import Common.Record;

/**
 * Represents an or node in the where tree.
 */
public class OrNode implements IWhereTree {

    private IWhereTree left;
    private IWhereTree right;

    public OrNode(IWhereTree left, IWhereTree right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public boolean evaluate(Record record, TableSchema schema) {
        return left.evaluate(record, schema) || right.evaluate(record, schema);
    }
}
