package StorageManager;

import Catalog.AttributeSchema;
import Catalog.TableSchema;
import java.util.List;

public class BPlusTree {

    private final Buffer buffer;
    private final TableSchema table;
    private final AttributeSchema indexedAttr;
    private final int attrIndex;
    private int rootPageId = -1;
    // private final int n;

    public BPlusTree(Buffer buffer, TableSchema table, AttributeSchema attr) {
        this.buffer = buffer;
        this.table = table;
        this.indexedAttr = attr;
        this.attrIndex = table.getAttributeIndex(attr.getName());
        // this.n = table.getBTreeN(); implement catalog and tableschema changes
    }

    public int getAttrIndex() {
        return attrIndex;
    }

    private BPlusTreeNode readNode(int pageId) {
        Page page = buffer.getPage(pageId);
        List<byte[]> records = page.getRecords();

        if (records.isEmpty()) {
            throw new IllegalStateException("B+ tree page " + pageId + " has no data.");
        }

        BPlusTreeNode node = BPlusTreeNode.fromBytes(records.get(0), indexedAttr);
        node.setPageId(pageId);

        return node;
    }

    private void writeNode(BPlusTreeNode node) {
        Page page = buffer.getPage(node.getPageId());
        page.cleanData();
        page.setNextPage(-1);
        byte[] data = node.toBytes(indexedAttr);

        if (page.addRecord(data) == false) {
            throw new IllegalStateException("B+ tree node data exceeds page capacity.");
        }
    }

    private BPlusTreeNode createNode(boolean isLeaf) {
        Page page = buffer.createNewPage();
        page.setNextPage(-1);
        return new BPlusTreeNode(page.getPageID(), isLeaf);
    }

    private BPlusTreeNode findLeafNode(Object key) {
        if (rootPageId == -1) {
            throw new IllegalStateException("Tree is empty.");
        }

        BPlusTreeNode node = readNode(rootPageId);

        while (!node.isLeaf()) {
            int childIndex = node.findChildIndex(key);
            int childPageId = node.getPointers().get(childIndex);
            node = readNode(childPageId);
        }
        return node;
    }

    public boolean contains(Object key) {
        if (key == null) return false;

        if (rootPageId == -1) return false;

        BPlusTreeNode leaf = findLeafNode(key);
        return leaf.findKeyIndex(key) != -1;
    }

    public boolean insert(Object key, int tablePageId) {
        // if (key == null)
        if (rootPageId == -1 ) {
            BPlusTreeNode root = createNode(true);
            root.insertLeafEntry(key, tablePageId);
            rootPageId = root.getPageId();
            writeNode(root);
            return true;
        }

        BPlusTreeNode leaf = findLeafNode(key);

        if (leaf.findKeyIndex(key) != -1) {
            return false;
        }
        leaf.insertLeafEntry(key, tablePageId);
        writeNode(leaf);

        return true;
    }
}
