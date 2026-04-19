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
                           boolean isUnique, Object defaultValue) {
        this.name = name.toLowerCase();
        this.type = dataType;
        this.isPrimaryKey = isPrimaryKey;
        this.isNotNull = isNotNull || isPrimaryKey;
        this.isUnique = isUnique || isPrimaryKey; // pk is always unique
        this.defaultValue = defaultValue;
    }
    public AttributeSchema(String name, DataType dataType,
                           boolean isPrimaryKey, boolean isNotNull,
                           Object defaultValue) {
        this(name, dataType, isPrimaryKey, isNotNull, false, defaultValue);
    }


    public boolean isUnique() {
        return isUnique;
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
                '}';
    }
}
