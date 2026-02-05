package DataTypes;

public class IntegerType implements DataType {

    @Override
    public String getTypeName() {
        return "INTEGER";
    }

    @Override
    public int getSize() {
        return Integer.BYTES;
    }

}
