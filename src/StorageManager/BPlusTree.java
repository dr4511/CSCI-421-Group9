package StorageManager;

import Catalog.AttributeSchema;
import Catalog.TableSchema;
import java.util.List;

public class BPlusTree {

    private final Buffer buffer;
    private final TableSchema table;
    private final AttributeSchema pkAttr;
    private final int pkIndex;
    // private final int n;

    public BPlusTree(Buffer buffer, TableSchema table) {
        this.buffer = buffer;
        this.table = table;
        this.pkAttr = table.getPrimaryKey();
        this.pkIndex = table.getAttributeIndex(pkAttr.getName());
        // this.n = table.getBTreeN(); implement catalog and tableschema changes
    }

    public int getPkIndex() {
        return pkIndex;
    }

    private BPlusTreeNode readNode(int pageId) {
        Page page = buffer.getPage(pageId);
        List<byte[]> records = page.getRecords();

        if (records.isEmpty()) {
            throw new IllegalStateException("B+ tree page " + pageId + " has no data.");
        }

        BPlusTreeNode node = BPlusTreeNode.fromBytes(records.get(0), pkAttr);
        node.setPageId(pageId);

        return node;
    }

    private void writeNode(BPlusTreeNode node) {
        Page page = buffer.getPage(node.getPageId());
        page.cleanData();
        page.setNextPage(-1);
        byte[] data = node.toBytes(pkAttr);

        if (page.addRecord(data) == false) {
            throw new IllegalStateException("B+ tree node data exceeds page capacity.");
        }
    }

    private BPlusTreeNode createNode(boolean isLeaf) {
        Page page = buffer.createNewPage();
        page.setNextPage(-1);
        return new BPlusTreeNode(page.getPageID(), isLeaf);
    }
}
