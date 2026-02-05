package Catalog;

import java.util.LinkedHashMap;

public class TableSchema {

    private String name;
    private LinkedHashMap<String, AttributeSchema> attributes;

    public TableSchema(String tableName) {
        this.name = tableName.toLowerCase();
        this.attributes = new LinkedHashMap<>();
    }

    public String getName() {
        return this.name;
    }

    public boolean addAttribute(AttributeSchema attribute) {
        String attrName = attribute.getName().toLowerCase();

        if (attributes.containsKey(attrName)) {
            return false;
        }

        attributes.put(name, attribute);
        return true;
    }

    public boolean dropAttribute(String attrName) {
        String name = attrName.toLowerCase();
        AttributeSchema attr = attributes.get(name);

        if (attr == null) {
            return false;
        }

        attributes.remove(name);
        return true;
    }
}
