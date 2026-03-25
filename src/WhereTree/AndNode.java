package WhereTree;

import Catalog.TableSchema;
import Common.Record;

/**
 * Represents an and node in where tree
 */
public class AndNode implements IWhereTree {
    
    private IWhereTree left;
    private IWhereTree right;
    
    public AndNode(IWhereTree left, IWhereTree right) {
        this.left = left;
        this.right = right;
    }
    
    @Override
    public boolean evaluate(Record record, TableSchema schema) {
        return left.evaluate(record, schema) && right.evaluate(record, schema);
    }
}
