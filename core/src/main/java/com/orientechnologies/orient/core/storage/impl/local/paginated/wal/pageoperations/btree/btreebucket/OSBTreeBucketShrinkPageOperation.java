package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OSBTreeBucketShrinkPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private List<byte[]> removedEntries;
  private int          newSize;
  private boolean      isEncrypted;

  public OSBTreeBucketShrinkPageOperation() {
  }

  public OSBTreeBucketShrinkPageOperation(List<byte[]> removedEntries, int newSize, boolean isEncrypted) {
    this.removedEntries = removedEntries;
    this.newSize = newSize;
    this.isEncrypted = isEncrypted;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SHRINK;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.shrink(newSize, isEncrypted);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    final int offset = page.size();
    final boolean isLeaf = page.isLeaf();
    if (isLeaf) {
      for (int i = 0; i < removedEntries.size(); i++) {
        final byte[] entry = removedEntries.get(i);
        page.insertLeafEntry(i + offset, entry);
      }
    } else {
      for (int i = 0; i < removedEntries.size(); i++) {
        final byte[] entry = removedEntries.get(i);
        page.insertNonLeafEntry(i + offset, entry, false);
      }
    }
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(removedEntries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (byte[] entry : removedEntries) {
      OIntegerSerializer.INSTANCE.serializeNative(entry.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(entry, 0, content, offset, entry.length);
      offset += entry.length;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);
    buffer.putInt(removedEntries.size());

    for (byte[] entry : removedEntries) {
      buffer.putInt(entry.length);
      buffer.put(entry);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int entriesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    removedEntries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      int entryLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      byte[] entry = new byte[entryLen];
      System.arraycopy(content, offset, entry, 0, entryLen);
      removedEntries.add(entry);

      offset += entryLen;
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int totalSize = (removedEntries.size() + 1) * OIntegerSerializer.INT_SIZE;
    for (byte[] entry : removedEntries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize;
  }
}
