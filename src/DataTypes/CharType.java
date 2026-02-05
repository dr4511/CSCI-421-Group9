package DataTypes;

public class CharType implements DataType {
    private int length;

    public CharType(int length)
    {
        this.length = length;
    }

    @Override
    public String getTypeName() {
        return "CHAR(" + length + ")";
    }

    @Override
    public int getSize() {
        return length + 1; // + 1 for \0
    }
}
