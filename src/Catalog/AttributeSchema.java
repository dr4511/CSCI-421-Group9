package Catalog;

import Common.DataType;

public class AttributeSchema {

    private final String name;
    private final DataType type;
    private final boolean isPrimaryKey;
    private final boolean isNotNull;
    private final Object defaultValue;
    private final boolean isUnique;

    public AttributeSchema(String name, DataType dataType,
                           boolean isPrimaryKey, boolean isNotNull,
                           Object defaultValue, boolean isUnique) {
        this.name = name.toLowerCase();  // Case-insensitive
        this.type = dataType;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull || isPrimaryKey;
        this.defaultValue = defaultValue;
        this.isUnique = isUnique;
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

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean getIsUnique() {
        return isUnique;
    }

    @Override
    public String toString() {
        String dv = (defaultValue == null)
                ? "null"
                : (defaultValue instanceof String ? "\"" + defaultValue + "\"" : String.valueOf(defaultValue));

        return "AttributeSchema{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", primaryKey=" + isPrimaryKey +
                ", notNull=" + isNotNull +
                ", defaultValue=" + dv +
                ", unique=" + isUnique +
                '}';
    }
}
