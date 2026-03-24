package WhereTree;

import Catalog.TableSchema;
import Common.Record;

public class AttrNode implements IOperandNode {
    private final String attributeName;

    public AttrNode(String attributeName) {
        this.attributeName = attributeName;
    }
    
    @Override
    public Object getValue(Record record, TableSchema table) {
        int index = table.getAttributeIndex(attributeName);
        return record.getValue(index);
    }
}