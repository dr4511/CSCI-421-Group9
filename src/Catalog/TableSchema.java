package Catalog;

import java.util.ArrayList;
import java.util.List;

public class TableSchema {

    private final String name;
    private final List<AttributeSchema> attributes;
    private int headPageId;

    public TableSchema(String tableName) {
        this.name = tableName.toLowerCase();
        this.attributes = new ArrayList<>();
        this.headPageId = -1;
    }

    public String getName() {
        return this.name;
    }

    public boolean addAttribute(AttributeSchema attr) {
        if (hasAttribute(attr.getName())) {
            return false;
        }
        attributes.add(attr);
        return true;
    }

    public boolean dropAttribute(String attrName) {
        return attributes.removeIf(a -> a.getName().equals(attrName.toLowerCase()));
    }

    public List<AttributeSchema> getAttributes() {
        return attributes;
    }

    public int getAttributeCount() {
        return attributes.size();
    }

    public AttributeSchema getAttribute(String attrName) {
        for (AttributeSchema attr : attributes) {
            if (attr.getName().equals(attrName.toLowerCase())) {
                return attr;
            }
        }
        return null;
    }

    public int getAttributeIndex(String attrName) {
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getName().equals(attrName.toLowerCase())) {
                return i;
            }
        }
        return -1;
    }

    public boolean hasAttribute(String attrName) {
        return getAttribute(attrName) != null;
    }

    public AttributeSchema getPrimaryKey() {
        for (AttributeSchema attr : attributes) {
            if (attr.isPrimaryKey()) {
                return attr;
            }
        }
        return null;
    }

    public int getHeadPageId() {
        return headPageId;
    }

    public void setHeadPageId(int pageId) {
        this.headPageId = pageId;
    }
}
