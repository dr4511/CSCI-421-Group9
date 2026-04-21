package StorageManager;

import Catalog.AttributeSchema;
import Catalog.TableSchema;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree {

    private final Buffer buffer;
    private final TableSchema table;
    private final AttributeSchema indexedAttr;
    private final int attrIndex;
    private final int n;

    public BPlusTree(Buffer buffer, TableSchema table, AttributeSchema attr) {
        this.buffer = buffer;
        this.table = table;
        this.indexedAttr = attr;
        this.attrIndex = table.getAttributeIndex(attr.getName());
        this.n = table.getBtreeN();
    }

    public int getPkIndex() {
        return attrIndex;
    }

    public int findTablePageForKey(Object key) {
        if (!table.hasBtreeIndex()) {
            return -1;
        }

        BPlusTreeNode leaf = findLeafNode(key);
        return leaf.findTablePageForKey(key);
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
        upsert(key, tablePageId);
    }

    /**
     * Inserts a new key or updates an existing key's data-page pointer.
     */
    public void upsert(Object key, int tablePageId) {
        if (!table.hasBtreeIndex()) {
            BPlusTreeNode root = createNode(true);
            root.insertLeafEntry(key, tablePageId);
            writeNode(root);
            table.setBtreeRootPageId(root.getPageId());
            return;
        }
        BPlusTreeNode leaf = findLeafNode(key);
        int existingIndex = leaf.findKeyIndex(key);
        if (existingIndex != -1) {
            leaf.getPointers().set(existingIndex, tablePageId);
            writeNode(leaf);
            return;
        }

        leaf.insertLeafEntry(key, tablePageId);
        if (!leaf.isOverfull(n)) {
            writeNode(leaf);
            return;
        }
        BPlusTreeNode parent = leaf.getPageId() == table.getBtreeRootPageId()
            ? null
            : findParentOf(table.getBtreeRootPageId(), leaf.getPageId(), null);
        splitAndPropagate(leaf, parent);
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
        return buffer.getBPlusTreeNode(pageId, indexedAttr);
    }

    private void writeNode(BPlusTreeNode node) {
        buffer.writeBPlusTreeNode(node, indexedAttr);
    }

    private BPlusTreeNode createNode(boolean isLeaf) {
        return buffer.createBPlusTreeNode(isLeaf);
    }
}
