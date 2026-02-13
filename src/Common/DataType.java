package Common;

import java.util.Objects;

public class DataType {

    public enum Type {
        INT, DOUBLE, BOOLEAN, CHAR, VARCHAR
    }

    private final Type type;
    private final int maxLength;

    public DataType(Type type) {
        this(type, -1);
    }

    public DataType(Type type, int maxLength) {
        this.type = type;
        this.maxLength = maxLength;
    }

    public Type getType() {
        return type;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if ((obj instanceof DataType) == false) return false;
        DataType other = (DataType) obj;
        return type == other.type && maxLength == other.maxLength;
    }

    public int hashCode() {
        return Objects.hash(type, maxLength);
    }

    public String toString() {
        switch (type) {
            case CHAR:
                return "CHAR(" + maxLength + ")";
            case VARCHAR:
                return "VARCHAR(" + maxLength + ")";
            default:
                return type.name();
        }
    }
}
