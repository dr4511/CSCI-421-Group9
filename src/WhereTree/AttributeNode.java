package WhereTree;

import Catalog.AttributeSchema;
import Catalog.TableSchema;
import Common.Record;

public class AttributeNode implements IOperandNode {
    private final String attributeName;

    public AttributeNode(String attributeName) {
        this.attributeName = attributeName;
    }
    
    @Override
    public Object getValue(Record record, TableSchema table) {

        try {
            AttributeSchema attr = table.resolveAttribute(attributeName);
            int index = table.getAttributeIndex(attr.getName());
            return record.getValue(index);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}