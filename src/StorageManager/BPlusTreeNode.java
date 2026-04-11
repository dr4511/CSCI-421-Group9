package StorageManager;

import Catalog.AttributeSchema;
import Common.DataType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeNode {

    public static final byte BTREE_MARKER = 0x01;

    private static final int NODE_HEADER_BYTES = 1 + 1 + Integer.BYTES + Integer.BYTES;

    private int pageId;
    private boolean isLeaf;
    private ArrayList<Object> keys;
    private ArrayList<Integer> pointers;
    private int siblingPageId;

    public BPlusTreeNode(int pageId, boolean isLeaf) {
        this.pageId = pageId;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        this.siblingPageId = -1;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public int getNumKeys() {
        return keys.size();
    }

    public List<Object> getKeys() {
        return keys;
    }

    public List<Integer> getPointers() {
        return pointers;
    }

    public int getSiblingPageId() {
        return siblingPageId;
    }

    public void setSiblingPageId(int siblingPageId) {
        this.siblingPageId = siblingPageId;
    }

    /**
     * insert (key, tablePagePtr) into leaf in sorted order
     */
    public void insertLeafEntry(Object key, int tablePageId) {
        int idx = findInsertIndex(key);
        keys.add(idx, key);
        pointers.add(idx, tablePageId);
    }

    /**
     * insert key + right child pointer into internal node pointer goes at
     * keyIndex + 1 because internals have one more ptr than keys
     */
    public void insertInternalEntry(Object key, int rightChildPageId) {
        int idx = findInsertIndex(key);
        keys.add(idx, key);
        pointers.add(idx + 1, rightChildPageId);
    }

    public boolean isOverfull(int n) {
        return keys.size() > n;
    }

    /**
     * internal node: which child pointer to follow
     */
    public int findChildIndex(Object searchKey) {
        for (int i = 0; i < keys.size(); i++) {
            if (compareKeys(searchKey, keys.get(i)) < 0) {
                return i;
            }
        }
        return keys.size();
    }

    /**
     * leaf: exact match, returns index or -1
     */
    public int findKeyIndex(Object searchKey) {
        for (int i = 0; i < keys.size(); i++) {
            if (compareKeys(searchKey, keys.get(i)) == 0) {
                return i;
            }
        }
        return -1;
    }

    /**
     * leaf: table page for the range searchKey falls into, -1 if empty
     */
    public int findTablePageForKey(Object searchKey) {
        if (pointers.isEmpty()) {
            return -1;
        }
        int bestIndex = 0;
        for (int i = 0; i < keys.size(); i++) {
            if (compareKeys(keys.get(i), searchKey) <= 0) {
                bestIndex = i;
            } else {
                break;
            }
        }
        return pointers.get(bestIndex);
    }

    public byte[] toBytes(AttributeSchema pkAttr) {
        int fixedKeySize = computeFixedKeySize(pkAttr);
        int numPtrs = isLeaf ? keys.size() : keys.size() + 1;

        int totalSize = NODE_HEADER_BYTES
            + keys.size() * fixedKeySize
            + numPtrs * Integer.BYTES;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.put(BTREE_MARKER);
        buffer.put((byte) (isLeaf ? 1 : 0));
        buffer.putInt(keys.size());
        buffer.putInt(siblingPageId);

        for (Object key : keys) {
            writeKey(buffer, key, pkAttr, fixedKeySize);
        }
        for (int ptr : pointers) {
            buffer.putInt(ptr);
        }

        return buffer.array();
    }

    public static BPlusTreeNode fromBytes(byte[] data, AttributeSchema pkAttr) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte marker = buffer.get();
        if (marker != BTREE_MARKER) {
            throw new IllegalStateException("Not a B+ tree node (marker: " + marker + ")");
        }

        boolean leaf = buffer.get() == 1;
        int numKeys = buffer.getInt();
        int sibling = buffer.getInt();
        int fixedKeySize = computeFixedKeySize(pkAttr);

        BPlusTreeNode node = new BPlusTreeNode(-1, leaf);
        node.siblingPageId = sibling;

        for (int i = 0; i < numKeys; i++) {
            node.keys.add(readKey(buffer, pkAttr, fixedKeySize));
        }

        int numPtrs = leaf ? numKeys : numKeys + 1;
        for (int i = 0; i < numPtrs; i++) {
            node.pointers.add(buffer.getInt());
        }

        return node;
    }

    /**
     * Fixed byte width of one search key slot. 
     * VARCHAR uses max length so N stays constant.
     */
    public static int computeFixedKeySize(AttributeSchema pkAttr) {
        DataType dataType = pkAttr.getDataType();
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
                return Integer.BYTES + dataType.getMaxLength();
            default:
                throw new IllegalArgumentException("Unsupported PK type: " + dataType.getType());
        }
    }

    /**
     * Compute order N of the B+ tree. Accounts for Page overhead since nodes are stored as Page records. 
     * N = (available space - extra ptr) / (keySize * + ptrSize)
     */
    public static int computeN(int pageSizeBytes, AttributeSchema pkAttr) {
        int pageOverhead = (Integer.BYTES * 5) + Long.BYTES + 1 + (Integer.BYTES * 2);

        int keySize = computeFixedKeySize(pkAttr);
        int ptrSize = Integer.BYTES;

        int available = pageSizeBytes - pageOverhead - NODE_HEADER_BYTES - ptrSize;
        return available / (keySize + ptrSize);
    }

    @SuppressWarnings("unchecked")
    public static int compareKeys(Object left, Object right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        Comparable<Object> comparableLeft = (Comparable<Object>) left;
        return comparableLeft.compareTo(right);
    }

    private int findInsertIndex(Object key) {
        for (int i = 0; i < keys.size(); i++) {
            if (compareKeys(key, keys.get(i)) < 0) {
                return i;
            }
        }
        return keys.size();
    }

    private static void writeKey(ByteBuffer buffer, Object key, AttributeSchema pkAttr, int fixedKeySize) {
        DataType dataType = pkAttr.getDataType();
        switch (dataType.getType()) {
            case INTEGER:
                buffer.putInt((Integer) key);
                break;
            case DOUBLE:
                buffer.putDouble((Double) key);
                break;
            case BOOLEAN:
                buffer.put((byte) ((Boolean) key ? 1 : 0));
                break;
            case CHAR: {
                int maxLen = dataType.getMaxLength();
                byte[] charBytes = new byte[maxLen];
                byte[] strBytes = ((String) key).getBytes();

                System.arraycopy(strBytes, 0, charBytes, 0, Math.min(strBytes.length, maxLen));
                buffer.put(charBytes);
                break;
            }
            case VARCHAR: {
                byte[] strBytes = ((String) key).getBytes();
                buffer.putInt(strBytes.length);
                buffer.put(strBytes);
                int padding = fixedKeySize - Integer.BYTES - strBytes.length;

                for (int i = 0; i < padding; i++) {
                    buffer.put((byte) 0);
                }
                break;
            }
        }
    }

    private static Object readKey(ByteBuffer buffer, AttributeSchema pkAttr, int fixedKeySize) {
        DataType dataType = pkAttr.getDataType();
        switch (dataType.getType()) {
            case INTEGER:
                return buffer.getInt();
            case DOUBLE:
                return buffer.getDouble();
            case BOOLEAN:
                return buffer.get() == 1;
            case CHAR: {
                int maxLen = dataType.getMaxLength();
                byte[] charBytes = new byte[maxLen];
                buffer.get(charBytes);
                String charStr = new String(charBytes);
                int endIdx = charStr.indexOf('\0');

                if (endIdx != -1) {
                    charStr = charStr.substring(0, endIdx);
                }
                return charStr.trim();
            }
            case VARCHAR: {
                int len = buffer.getInt();
                byte[] strBytes = new byte[len];
                buffer.get(strBytes);

                int padding = fixedKeySize - Integer.BYTES - len;
                buffer.position(buffer.position() + padding);
                return new String(strBytes);
            }
            default:
                return null;
        }
    }

    @Override
    public String toString() {
        return "BPlusTreeNode{pageId=" + pageId + ", isLeaf=" + isLeaf + ", numKeys=" + keys.size() + ", sibling=" + siblingPageId + '}';
    }
}
