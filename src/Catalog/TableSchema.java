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

    /**
     * @return the name of the table (case-insensitive)
     */
    public String getName() {
        return this.name;
    }

    /**
     * Adds an attribute to the table schema.
     * @return false if an attribute with the same name already exists.
     */
    public boolean addAttribute(AttributeSchema attr) {
        if (hasAttribute(attr.getName())) {
            return false;
        }
        attributes.add(attr);
        return true;
    }

    /**
     * Removes the attribute with the given name from the table schema.
     * @return false if no attribute with the given name exists.
     */
    public boolean dropAttribute(String attrName) {
        return attributes.removeIf(a -> a.getName().equals(attrName.toLowerCase()));
    }

    /**
     * @return the list of attributes in the table schema, in the order they were added.
     */
    public List<AttributeSchema> getAttributes() {
        return attributes;
    }

    /**
     * @return the number of attributes in the table schema.
     */
    public int getAttributeCount() {
        return attributes.size();
    }

    /**
     * @return the AttributeSchema for the attribute with the given name, or null if no attribute exists.
     */
    public AttributeSchema getAttribute(String attrName) {
        for (AttributeSchema attr : attributes) {
            if (attr.getName().equals(attrName.toLowerCase())) {
                return attr;
            }
        }
        return null;
    }

    /**
     * @return the index of the attribute with the given name, or -1 if no attribute exists.
     */
    public int getAttributeIndex(String attrName) {
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getName().equals(attrName.toLowerCase())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Checks if the table schema has an attribute with the given name.
     */
    public boolean hasAttribute(String attrName) {
        return getAttribute(attrName) != null;
    }

    /**
     * @return the AttributeSchema for the primary key attribute, or null if no primary key is defined.
     */
    public AttributeSchema getPrimaryKey() {
        for (AttributeSchema attr : attributes) {
            if (attr.isPrimaryKey()) {
                return attr;
            }
        }
        return null;
    }

    /**
     * @return the page ID of the head page for this table, or -1 if not set.
     */
    public int getHeadPageId() {
        return headPageId;
    }

    /**
     * Sets the page ID of the head page.
     */
    public void setHeadPageId(int pageId) {
        this.headPageId = pageId;
    }
}
