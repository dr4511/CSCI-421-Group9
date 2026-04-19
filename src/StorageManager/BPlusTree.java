package StorageManager;

import Catalog.AttributeSchema;
import Catalog.TableSchema;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree {

    private final Buffer buffer;
    private final TableSchema table;
    private final AttributeSchema pkAttr;
    private final int pkIndex;
    private final int n;

    public BPlusTree(Buffer buffer, TableSchema table) {
        this.buffer = buffer;
        this.table = table;
        this.pkAttr = table.getPrimaryKey();
        this.pkIndex = table.getAttributeIndex(pkAttr.getName());
        this.n = table.getBtreeN();
    }

    public int getPkIndex() {
        return pkIndex;
    }

    public int search(Object key) {
        if (!table.hasBtreeIndex()) {
            return -1;
        }
        BPlusTreeNode leaf = findLeafNode(key);
        int idx = leaf.findKeyIndex(key);

        if (idx == -1) {
            return -1;
        }

        return leaf.getPointers().get(idx);
    }

    /**
     * returns true if key exists in the tree
     */
    public boolean contains(Object key) {
        if (!table.hasBtreeIndex()) {
            return false;
        }
        BPlusTreeNode leaf = findLeafNode(key);
        return leaf.findKeyIndex(key) != -1;
    }

    /**
     * Inserts into the B+ tree.
     */
    public void insert(Object key, int tablePageId) {
        if (!table.hasBtreeIndex()) {
            BPlusTreeNode root = createNode(true);
            root.insertLeafEntry(key, tablePageId);
            writeNode(root);
            table.setBtreeRootPageId(root.getPageId());
            return;
        }
        BPlusTreeNode leaf = findLeafNode(key);
        leaf.insertLeafEntry(key, tablePageId);
        if (!leaf.isOverfull(n)) {
            writeNode(leaf);
            return;
        }
        splitAndPropagate(leaf, null);
    }

    /**
     * traverses tree from the root down to the leaf node
     * that contains given key.
     */
    public BPlusTreeNode findLeafNode(Object key) {
        BPlusTreeNode current = readNode(table.getBtreeRootPageId());
        while (!current.isLeaf()) {
            int childIndex = current.findChildIndex(key);
            int childPageId = current.getPointers().get(childIndex);
            current = readNode(childPageId);
        }

        return current;
    }

    /**
     * splits an overfull node and propagates the split up to parent
     */
    private void splitAndPropagate(BPlusTreeNode overfullNode, BPlusTreeNode parentNode) {
        if (overfullNode.isLeaf()) {
            splitLeaf(overfullNode, parentNode);
        } else {
            splitInternal(overfullNode, parentNode);
        }
    }

    /**
     * splits an overfull leaf node into two leaf nodes.
     */
    private void splitLeaf(BPlusTreeNode leaf, BPlusTreeNode parent) {
        List<Object> allKeys = new ArrayList<>(leaf.getKeys());
        List<Integer> allPtrs = new ArrayList<>(leaf.getPointers());
        int mid = (int) Math.ceil((double) allKeys.size() / 2);
        BPlusTreeNode leftLeaf = leaf;
        leftLeaf.getKeys().clear();
        leftLeaf.getPointers().clear();
        for (int i = 0; i < mid; i++) {
            leftLeaf.getKeys().add(allKeys.get(i));
            leftLeaf.getPointers().add(allPtrs.get(i));
        }
        BPlusTreeNode rightLeaf = createNode(true);
        for (int i = mid; i < allKeys.size(); i++) {
            rightLeaf.getKeys().add(allKeys.get(i));
            rightLeaf.getPointers().add(allPtrs.get(i));
        }
        rightLeaf.setSiblingPageId(leftLeaf.getSiblingPageId());
        leftLeaf.setSiblingPageId(rightLeaf.getPageId());
        writeNode(leftLeaf);
        writeNode(rightLeaf);
        Object pushUpKey = rightLeaf.getKeys().get(0);
        insertInParent(leftLeaf, pushUpKey, rightLeaf, parent);
    }

    /**
     * splits an overfull internal node into two internal nodes.
     */
    private void splitInternal(BPlusTreeNode internal, BPlusTreeNode parent) {
        List<Object> allKeys = new ArrayList<>(internal.getKeys());
        List<Integer> allPtrs = new ArrayList<>(internal.getPointers());
        int midKeyIdx = (int) Math.ceil((double)(n + 1) / 2) - 1;
        Object pushUpKey = allKeys.get(midKeyIdx);
        BPlusTreeNode leftInternal = internal;
        leftInternal.getKeys().clear();
        leftInternal.getPointers().clear();
        for (int i = 0; i < midKeyIdx; i++) {
            leftInternal.getKeys().add(allKeys.get(i));
        }
        for (int i = 0; i <= midKeyIdx; i++) {
            leftInternal.getPointers().add(allPtrs.get(i));
        }
        BPlusTreeNode rightInternal = createNode(false);
        for (int i = midKeyIdx + 1; i < allKeys.size(); i++) {
            rightInternal.getKeys().add(allKeys.get(i));
        }
        for (int i = midKeyIdx + 1; i < allPtrs.size(); i++) {
            rightInternal.getPointers().add(allPtrs.get(i));
        }
        writeNode(leftInternal);
        writeNode(rightInternal);
        insertInParent(leftInternal, pushUpKey, rightInternal, parent);
    }

    /**
     * inserts a pushed-up key and new right child into the parent node after
     * a leaf or internal node split
     */
    private void insertInParent(BPlusTreeNode leftChild, Object pushUpKey, BPlusTreeNode rightChild, BPlusTreeNode parent) {
        if (parent == null) {
            BPlusTreeNode newRoot = createNode(false);
            newRoot.getPointers().add(leftChild.getPageId());
            newRoot.insertInternalEntry(pushUpKey, rightChild.getPageId());
            writeNode(newRoot);
            table.setBtreeRootPageId(newRoot.getPageId());
            return;
        }
        parent.insertInternalEntry(pushUpKey, rightChild.getPageId());

        if (!parent.isOverfull(n)) {
            writeNode(parent);
            return;
        }
        BPlusTreeNode parentOfParent = findParentOf(table.getBtreeRootPageId(), parent.getPageId(), null);
        splitAndPropagate(parent, parentOfParent);
    }


    private BPlusTreeNode findParentOf(int currentPageId, int targetPageId, BPlusTreeNode currentParent) {
        if (currentPageId == targetPageId) {
            return currentParent;
        }
        BPlusTreeNode current = readNode(currentPageId);
        if (current.isLeaf()) {
            return null;
        }
        for (int childPageId : current.getPointers()) {
            BPlusTreeNode result = findParentOf(childPageId, targetPageId, current);
            if (result != null || childPageId == targetPageId) {
                return result != null ? result : current;
            }
        }
        return null;
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

        if (!page.addRecord(data)) {
            throw new IllegalStateException("B+ tree node data exceeds page capacity.");
        }
    }

    private BPlusTreeNode createNode(boolean isLeaf) {
        Page page = buffer.createNewPage();
        page.setNextPage(-1);
        return new BPlusTreeNode(page.getPageID(), isLeaf);
    }
}