package DataTypes;

public class BooleanType implements DataType {

    @Override
    public String getTypeName() {
        return "BOOLEAN";
    }

    @Override
    public int getSize() {
        return 1;
    }
}
