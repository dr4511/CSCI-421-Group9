package DataTypes;

public class DoubleType implements DataType {

    @Override
    public String getTypeName() {
        return "DOUBLE";
    }

    @Override
    public int getSize() {
        return Double.BYTES;
    }
}
