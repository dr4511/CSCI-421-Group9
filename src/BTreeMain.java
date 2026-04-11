
import Catalog.AttributeSchema;
import Common.DataType;
import StorageManager.BPlusTreeNode;

public class BTreeMain {

    public static void main(String[] args) {
        AttributeSchema pk = new AttributeSchema("id", new DataType(DataType.Type.INTEGER), true, true, null);

        int n = BPlusTreeNode.computeN(4096, pk);
        System.out.println("N for INTEGER PK, 4096 page: " + n);

        BPlusTreeNode leaf = new BPlusTreeNode(0, true);
        leaf.insertLeafEntry(30, 100);
        leaf.insertLeafEntry(10, 200);
        leaf.insertLeafEntry(20, 300);

        byte[] bytes = leaf.toBytes(pk);
        BPlusTreeNode restored = BPlusTreeNode.fromBytes(bytes, pk);
        restored.setPageId(0);

        System.out.println("Keys: " + restored.getKeys());
        System.out.println("Pointers: " + restored.getPointers());

        BPlusTreeNode internal = new BPlusTreeNode(1, false);
        internal.getPointers().add(10); // leftmost child
        internal.insertInternalEntry(20, 11);
        internal.insertInternalEntry(40, 12);

        System.out.println("child for 5: " + internal.findChildIndex(5));
        System.out.println("child for 25: " + internal.findChildIndex(25));
        System.out.println("child for 50: " + internal.findChildIndex(50));

        System.out.println("overfull n=2: " + internal.isOverfull(2));
        System.out.println("overfull n=1: " + internal.isOverfull(1));
    }
}
