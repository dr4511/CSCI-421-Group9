package WhereTree;

import Catalog.TableSchema;
import Common.Record;

public class AttributeNode implements IOperandNode {
    private final String attributeName;

    public AttributeNode(String attributeName) {
        this.attributeName = attributeName;
    }
    
    @Override
    public Object getValue(Record record, TableSchema table) {
        int index = table.getAttributeIndex(attributeName);
        return record.getValue(index);
    }
}