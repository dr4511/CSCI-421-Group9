package StorageManager;

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class Page implements Comparable<Page> {

    private static class Slot {
        int offset;
        int length;
        Slot(int offset, int length) {
            this.offset = offset;
            this.length = length;
        }
    }

    private int pageSize;
    private int pageID;
    private byte[] dataArea;
    private List<Slot> slots;
    private int freeSpaceEnd;  // grows backward
    private int nextPageID;
    private long lastAccessTimestamp;
    private boolean isDirty;


    public Page(int pageID, int pageSize) {
        this.pageSize = pageSize;
        this.dataArea = new byte[pageSize];
        this.slots = new ArrayList<>();
        this.freeSpaceEnd = pageSize;
        this.nextPageID = -1;              // -1 means no nextPage
        touch();
    }

    // Use whever page is accessed (read or write)
    // USE WHEN: Loaded in buffer, Record added, Record removed, Record read
    public void touch() {
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

// Attempt to add record. Returns true if successful.
// Splits page if not enough space.
public boolean addRecord(byte[] record) {
    int slotSize = 2 * Integer.BYTES;
    int recordSize = record.length;

    // Fits in current page
    if (getFreeSpace() >= recordSize + slotSize) {
        insertRecordInternal(record); // always calls Slot constructor
        return true;
    }
    // Too big for single page
    return false;
}

// Helper to copy data and slots from another page (after split)
private void copyFromPage(Page source) {
    this.dataArea = source.dataArea;
    this.slots = source.slots;
    this.freeSpaceEnd = source.freeSpaceEnd;
    this.nextPageID = source.nextPageID;
}
    private void insertRecordInternal(byte[] record) {
    int recordSize = record.length;
    freeSpaceEnd -= recordSize;
    System.arraycopy(record, 0, dataArea, freeSpaceEnd, recordSize);

    // Slot constructor is called here
    Slot slot = new Slot(freeSpaceEnd, recordSize);
    slots.add(slot);
    this.touch();
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
        touch();

        return true;
    }

    // Split page due to insufficient space. Does so by creating two new pages and halving data.
    // SPLIT IN STORAGEMANAGER will call page.split, with two page objects
    // no need to return anything as page's info inside object 
    // MAKE SURE isDirty IS BEING SET and UNSET (unset by buffer right before writing)
    public Page[] split(Page first, Page second) {

        this.setDirty();

        int mid = slots.size() / 2;

        // copy first half
        for (int i = 0; i < mid; i++) {
            Slot s = slots.get(i);
            byte[] record = new byte[s.length];
            System.arraycopy(dataArea, s.offset, record, 0, s.length);
            first.insertRecordInternal(record);
        }

        // copy second half
        for (int i = mid; i < slots.size(); i++) {
            Slot s = slots.get(i);
            byte[] record = new byte[s.length];
            System.arraycopy(dataArea, s.offset, record, 0, s.length);
            second.insertRecordInternal(record);
        }

        Page[] result = new Page[2];
        result[0] = first;
        result[1] = second;

        return result;
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

    public int getPageID() {
        return this.pageID;
    }

    public int getNextPage() {
        return nextPageID;
    }

    public void setNextPage(int next) {
        this.nextPageID = next;
        this.setDirty();
    }

    public boolean hasNextPage() {
        if(this.nextPageID == -1){
            return false;
        }
        return true;
    }

    public boolean isDirty() {
        return this.isDirty;
    }

    public void setDirty() {
        this.isDirty = true;
    }

    public void cleanDirty() {
        this.isDirty = false;
    }

    public ByteBuffer serializePage() {
        int slotCount = slots.size();

        int headerSize = Integer.BYTES * 5    // pageSize, freeSpaceEnd, slotCount, slotCount, nextPageID
                                        + Long.BYTES  // lastAccessTimestamp
                                        + 1;  // dirty Flag
        int slotSectionSize = slotCount * (2 * Integer.BYTES);
        
        int totalSize = headerSize + slotSectionSize;


        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Header
        buffer.putInt(pageID);
        buffer.putInt(pageSize);
        buffer.putInt(freeSpaceEnd);
        buffer.putInt(slotCount);
        buffer.putInt(nextPageID);
        buffer.putLong(lastAccessTimestamp);
        buffer.put((byte) (isDirty ? 1 : 0));

        // Slot directory
        for (Slot s : slots) {
        buffer.putInt(s.offset);
        buffer.putInt(s.length);
        }

        // Data area
        buffer.put(dataArea);

        buffer.flip();
        return buffer;
    }

    public static Page deserializePage(ByteBuffer buffer) {
        buffer.rewind();

        // Header
        int pageID = buffer.getInt();
        int pageSize = buffer.getInt();
        int freeSpaceEnd = buffer.getInt();
        int slotCount = buffer.getInt();
        int nextPageID = buffer.getInt();
        long lastAccessTimestamp = buffer.getLong();
        boolean dirty = buffer.get() == 1;

        Page page = new Page(pageID,pageSize);
        page.freeSpaceEnd = freeSpaceEnd;
        page.nextPageID = nextPageID;
        page.lastAccessTimestamp = lastAccessTimestamp;
        page.isDirty = dirty;

        // Slots
        for (int i = 0; i < slotCount; i++) {
            int offset = buffer.getInt();
            int length = buffer.getInt();
            page.slots.add(new Slot(offset, length));
        }

        // Data Area
        buffer.get(page.dataArea);
        return page;
    }


    @Override
    public int compareTo(Page other) {
        return Long.compare(this.lastAccessTimestamp,other.lastAccessTimestamp);
    }

}
