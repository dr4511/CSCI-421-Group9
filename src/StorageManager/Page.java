package StorageManager;

import java.util.ArrayList;
import java.util.List;

import Catalog.AttributeSchema;

import java.nio.ByteBuffer;
import Common.Record;

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
    private ArrayList<Record> records;
    private List<Slot> slots;
    private int freeSpaceEnd;  // grows backward
    private int nextPageID;
    private long lastAccessTimestamp;
    private boolean isDirty;


    public Page(int pageID, int pageSize) {
        this.pageSize = pageSize;
        this.records = new ArrayList<Record>();
        this.slots = new ArrayList<>();
        this.freeSpaceEnd = pageSize;
        this.nextPageID = -1;              // -1 means no nextPage
        touch();
    }

    // Removes all data from page
    public void cleanData(){
        this.records = new ArrayList<Record>();
        this.slots = new ArrayList<>();
        this.freeSpaceEnd = pageSize;
        this.nextPageID = -1;
        touch();
    }

    // Use whever page is accessed (read or write)
    // USE WHEN: Loaded in buffer, Record added, Record removed, Record read
    public void touch() {
        this.lastAccessTimestamp = System.currentTimeMillis();
    }

// Attempt to add record. Returns true if successful.
// Splits page if not enough space.
public boolean addRecord(Record record, int recordSizeBytes) {
    int slotSize = 2 * Integer.BYTES;

    // Fits in current page
    if (getFreeSpace() >= recordSizeBytes + slotSize) {
        insertRecordInternal(record, recordSizeBytes); // always calls Slot constructor
        return true;
    }
    // Too big for single page
    return false;
}

    private void insertRecordInternal(Record record, int recordSize) {
    freeSpaceEnd -= recordSize;
    this.records.add(record);

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

        records.remove(slotIndex);  // TODO verify data and slots are aligned after removal
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
    public Page[] split(Page first, Page second, int incomingRecordSize) {

        this.setDirty();

        int mid = slots.size() / 2;

        // copy first half
        for (int i = 0; i < mid; i++) {
            Slot s = slots.get(i); // IS THIS NEEDED
            Record record = records.get(i);
            first.insertRecordInternal(record, incomingRecordSize); 
        }

        // copy second half
        for (int i = mid; i < slots.size(); i++) {
            Slot s = slots.get(i);  // IS THIS NEEDED
            Record record = records.get(i);
            second.insertRecordInternal(record, incomingRecordSize); 
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

    public List<Record> getRecords() {
        return this.records;
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
        int dataSize = 0;
        for(int i = 0; i < slotCount;i++){
            dataSize += slots.get(i).length;
        }


        ByteBuffer buffer = ByteBuffer.allocate(pageSize);

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
        for(Record record : records){
            buffer.put(record.toBytes()); // TODO GET SCHEMA PASSED IN
        }

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

        page.records = new ArrayList<Record>();
        // Data Area
        for(int i = 0; i < slotCount;i++){
            int numBytes = page.slots.get(i).length;
            Record newRecord = Record.fromBytes(buffer.get(numBytes), null); // TODO GET SCHEMA PASSED IN
            page.records.add(newRecord); 
        }
        return page;
    }


    @Override
    public int compareTo(Page other) {
        return Long.compare(this.lastAccessTimestamp,other.lastAccessTimestamp);
    }

}
