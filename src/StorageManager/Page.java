package StorageManager;

import java.util.ArrayList;
import java.util.List;

public class Page {

    private static class Slot {
        int offset;
        int length;
        Slot(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }

    private int pageSize;
    private byte[] dataArea;
    private List<Slot> slots;
    private int freeSpaceEnd;  // grows backward
    private Page nextPage;


    public Page(int pageSize) {
        this.pageSize = pageSize;
        this.dataArea = new byte[pageSize];
        this.slots = new ArrayList<>();
        this.freeSpaceEnd = pageSize;
        this.nextPage = null;
    }

    // Attempt to add record. Returns true if successful.
    // Splits page if not enough space.
    public boolean addRecord(byte[] record) {
        int recordSize = record.length;
        int slotSize = 2 * Integer.BYTES;

        if (getFreeSpace() < recordSize + slotSize) {
            return false; // not enough space
        }

        // insert record at end of free space
        freeSpaceEnd -= recordSize;
        System.arraycopy(record, 0, dataArea, freeSpaceEnd, recordSize);

        // add slot
        slots.add(new Slot(freeSpaceEnd, recordSize));

        return true;
    }

    // Attempt to remove record. Returns true if successful.
    public boolean removeRecord(int slotIndex) {
        if (slotIndex < 0 || slotIndex >= slots.size()) return false;

        Slot slot = slots.get(slotIndex);
        int start = slot.offset;
        int length = slot.length;

        // compact data area: shift bytes above this record forward
        int shiftAmount = length;
        for (Slot s : slots) {
            if (s.offset < start) {
                s.offset += shiftAmount;
            }
        }

        System.arraycopy(dataArea, 0, dataArea, shiftAmount, start);
        freeSpaceEnd += length;

        // remove slot
        slots.remove(slotIndex);

        return true;
    }

    // Split page due to insufficient space. Does so by creating two new pages and halving data.
    // Returns new page that points to another page which inherits the original's nextPage.
    public Page split() {
        Page first = new Page(pageSize);
        Page second = new Page(pageSize);

        int mid = slots.size() / 2;

        // first half
        for (int i = 0; i < mid; i++) {
            Slot s = slots.get(i);
            byte[] record = new byte[s.length];
            System.arraycopy(dataArea, s.offset, record, 0, s.length);
            first.addRecord(record);
        }

        // second half
        for (int i = mid; i < slots.size(); i++) {
            Slot s = slots.get(i);
            byte[] record = new byte[s.length];
            System.arraycopy(dataArea, s.offset, record, 0, s.length);
            second.addRecord(record);
        }

        first.setNextPage(second);
        second.setNextPage(this.nextPage);

        return first;
    }


    public int getFreeSpace() {
        int slotSize = 2 * Integer.BYTES;
        return freeSpaceEnd - (slots.size() * slotSize);
    }

    public int getNumRecords() {
        return slots.size();
    }

    public List<byte[]> getRecords() {
        List<byte[]> list = new ArrayList<>();
        for (Slot s : slots) {
            byte[] rec = new byte[s.length];
            System.arraycopy(dataArea, s.offset, rec, 0, s.length);
            list.add(rec);
        }
        return list;
    }

    public Page getNextPage() {
        return nextPage;
    }

    public void setNextPage(Page next) {
        this.nextPage = next;
    }
}
