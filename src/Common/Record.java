package Common;

import Catalog.TableSchema;
import Catalog.AttributeSchema;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Represents a single record
 * Handles serialization/deserialization as well as variable len records
 */
public class Record {
    // arr holding the actual values for each attribute in this record
    private Object[] values;
    
    // bitmap tracking for which attributes are null
    // used during serialization so we know which values to skip
    private boolean[] nullBitmap;
    
    /**
     * Creates an empty record with space for numAttributes values.
     * all values start as null
     */
    public Record(int numAttributes) {
        this.values = new Object[numAttributes];
        this.nullBitmap = new boolean[numAttributes];
    }
    
    /**
     * create a record from an existing array of values
     * automatically build null bitmap based on which values are null
     */
    public Record(Object[] values) {
        this.values = values;
        this.nullBitmap = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            nullBitmap[i] = (values[i] == null);
        }
    }
    
    /**
     * set the value at a specific attribute index
     * updates the null bitmap accordingly
     */
    public void setValue(int index, Object value) {
        values[index] = value;
        nullBitmap[index] = (value == null);
    }
    
    /**
     * Get the value at a specific attribute index
     * Returns null if attribute is null
     */
    public Object getValue(int index) {
        return values[index];
    }
    
    /**
     * check if a specific attribute is null
     */
    public boolean isNull(int index) {
        return nullBitmap[index];
    }
    
    /**
     * get all values in this record
     */
    public Object[] getValues() {
        return values;
    }
    
    /**
     * get num of attributes in this record
     */
    public int getNumAttributes() {
        return values.length;
    }
    
    /**
     * Serialize this record to binary format for storage on disk
     */
    public byte[] toBytes(TableSchema schema) {
        // calculate total size needed
        int totalSize = calculateNullBitmapSize(values.length);
        // add attribute sizes
        for (int i = 0; i < values.length; i++) {
            if (!nullBitmap[i]) {
                AttributeSchema attr = schema.getAttributes().get(i);
                totalSize += calculateAttributeSize(attr, values[i]);
            }
        }
        // buffer to hold all the bytes
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        // write the null bitmap
        writeNullBitmap(buffer, nullBitmap);
        // write each attribute value in order
        for (int i = 0; i < values.length; i++) {
            if (!nullBitmap[i]) {
                AttributeSchema attr = schema.getAttributes().get(i);
                writeValue(buffer, values[i], attr);
            }
        }
        // return complete byte array
        return buffer.array();
    }
    
    /**
     * Deserialize a record from binary format
     */
    public static Record fromBytes(byte[] data, TableSchema schema) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int numAttributes = schema.getAttributes().size();
        // create a empty record
        Record record = new Record(numAttributes);
        // read the null bitmap
        boolean[] nullBitmap = readNullBitmap(buffer, numAttributes);
        record.nullBitmap = nullBitmap;
        // read each attribute value
        for (int i = 0; i < numAttributes; i++) {
            if (!nullBitmap[i]) {
                AttributeSchema attr = schema.getAttributes().get(i);
                record.values[i] = readValue(buffer, attr);
            } else {
                record.values[i] = null;
            }
        }
        return record;
    }
    
    /**
     * Calculate num of bytes needed to store a single attribute value
     */
    private static int calculateAttributeSize(AttributeSchema attr, Object value) {
        DataType dataType = attr.getDataType();
        switch (dataType.getType()) {
            case INTEGER:
                return Integer.BYTES;
            case DOUBLE:
                return Double.BYTES;
            case BOOLEAN:
                return 1;
            case CHAR:
                return dataType.getMaxLength();
            case VARCHAR:
                String str = (String) value;
                return Short.BYTES + str.length();
            default:
                return 0;
        }
    }
    
    /**
     * Calculate how many bytes are needed for the null bitmap
     */
    private static int calculateNullBitmapSize(int numAttributes) {
        return (numAttributes + 7) / 8;
    }
    
    /**
     * Write null bitmap to the buffer.
     */
    private static void writeNullBitmap(ByteBuffer buffer, boolean[] nullBitmap) {
        int numBytes = calculateNullBitmapSize(nullBitmap.length);
        for (int byteIdx = 0; byteIdx < numBytes; byteIdx++) {
            byte b = 0;
            for (int bitIdx = 0; bitIdx < 8 && (byteIdx * 8 + bitIdx) < nullBitmap.length; bitIdx++) {
                if (nullBitmap[byteIdx * 8 + bitIdx]) {
                    b |= (1 << bitIdx);
                }
            }
            buffer.put(b);
        }
    }
    
    /**
     * Read the null bitmap from the buffer
     * Reads the appropriate number of bytes and extracts the bit for each attribute.
     */
    private static boolean[] readNullBitmap(ByteBuffer buffer, int numAttributes) {
        boolean[] nullBitmap = new boolean[numAttributes];
        int numBytes = calculateNullBitmapSize(numAttributes);
        for (int byteIdx = 0; byteIdx < numBytes; byteIdx++) {
            byte b = buffer.get();
            for (int bitIdx = 0; bitIdx < 8 && (byteIdx * 8 + bitIdx) < numAttributes; bitIdx++) {
                nullBitmap[byteIdx * 8 + bitIdx] = ((b >> bitIdx) & 1) == 1;
            }
        }
        return nullBitmap;
    }
    
    /**
     * Write a single attribute value
     */
    private static void writeValue(ByteBuffer buffer, Object value, AttributeSchema attr) {
        DataType dataType = attr.getDataType();
        switch (dataType.getType()) {
            case INTEGER:
                // 4 bytes for integer
                buffer.putInt((Integer) value);
                break;
                
            case DOUBLE:
                // 8 bytes for double
                buffer.putDouble((Double) value);
                break;
                
            case BOOLEAN:
                buffer.put((byte) ((Boolean) value ? 1 : 0));
                break;
                
            case CHAR:
                String charStr = (String) value;
                int maxLen = dataType.getMaxLength();
                byte[] charBytes = new byte[maxLen];  // Create array of N bytes
                byte[] strBytes = charStr.getBytes(StandardCharsets.UTF_8);
                
                // Copy string bytes into the array (rest stays as zeros)
                System.arraycopy(strBytes, 0, charBytes, 0, Math.min(strBytes.length, maxLen));
                buffer.put(charBytes);
                break;
                
            case VARCHAR:
                // varchar is variable length
                String varcharStr = (String) value;
                byte[] varcharBytes = varcharStr.getBytes(StandardCharsets.UTF_8);
                buffer.putShort((short) varcharBytes.length);
                buffer.put(varcharBytes);
                break;
        }
    }
    
    /**
     * Read a attribute value
     */
    private static Object readValue(ByteBuffer buffer, AttributeSchema attr) {
        DataType dataType = attr.getDataType();
        switch (dataType.getType()) {
            case INTEGER:
                // read 4 bytes and convert to int
                return buffer.getInt();
                
            case DOUBLE:
                return buffer.getDouble();
                
            case BOOLEAN:
                return buffer.get() == 1;
                
            case CHAR:
                int maxLen = dataType.getMaxLength();
                byte[] charBytes = new byte[maxLen];
                buffer.get(charBytes);
                String charStr = new String(charBytes, StandardCharsets.UTF_8);
                int endIdx = charStr.indexOf('\0');  // Find first null byte
                if (endIdx != -1) {
                    charStr = charStr.substring(0, endIdx);
                }
                return charStr.trim();
            case VARCHAR:
                short length = buffer.getShort();
                byte[] varcharBytes = new byte[length];
                buffer.get(varcharBytes);
                return new String(varcharBytes, StandardCharsets.UTF_8);
                
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
