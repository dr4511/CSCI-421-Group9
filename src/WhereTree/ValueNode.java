package WhereTree;

import Catalog.TableSchema;
import Common.Record;

public class ValueNode implements IOperandNode {
    private final Object value;

    public ValueNode(Object value) {
        this.value = value;
    }

    @Override
    public Object getValue(Record record, TableSchema table) {
        return value;
    }
}