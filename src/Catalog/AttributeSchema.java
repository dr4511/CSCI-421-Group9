package Catalog;

import DataTypes.DataType;

public class AttributeSchema {

    private String name;
    private DataType type;
    private boolean isPrimaryKey;
    private boolean isNotNull;

    public AttributeSchema(String name, DataType dataType,
                           boolean isPrimaryKey, boolean isNotNull,
                           Object defaultValue) {
        this.name = name.toLowerCase();  // Case-insensitive
        this.type = dataType;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return type;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isNotNull() {
        return isNotNull;
    }
}
