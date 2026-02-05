package DataTypes;

public interface DataType {

    /**
     * @return String representation of data type
     */
    public String getTypeName();

    /**
     * @return Size in bytes of variable
     */
    public int getSize();
}
