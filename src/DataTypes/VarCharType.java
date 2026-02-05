package DataTypes;

public class VarCharType implements DataType {
    private int length;

    public VarCharType(int length)
    {
        this.length = length;
    }

    @Override
    public String getTypeName() {
        return "VARCHAR(" + length + ")";
    }

    @Override
    public int getSize() {
        return length + 1;
    }
}
