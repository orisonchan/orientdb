package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class OSBTreeBucketAddAllPageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private List<byte[]> entries;

  public OSBTreeBucketAddAllPageOperation() {
  }

  public OSBTreeBucketAddAllPageOperation(List<byte[]> entries) {
    this.entries = entries;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_ADD_ALL;
  }

  @Override
  protected OSBTreeBucket createPageInstance(OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doRedo(OSBTreeBucket page) {
    page.addAll(entries);
  }

  @Override
  protected void doUndo(OSBTreeBucket page) {
    page.shrink(0, false);// does not matter whether we encrypt data or not if we remove all data
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(entries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (byte[] entry : entries) {
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

    buffer.putInt(entries.size());
    for (byte[] entry : entries) {
      buffer.putInt(entry.length);
      buffer.put(entry);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int entriesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    entries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      int entrySize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] entry = new byte[entrySize];
      System.arraycopy(content, offset, entry, 0, entriesSize);
      offset += entrySize;

      entries.add(entry);
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    int totalSize = (entries.size() + 1) * OIntegerSerializer.INT_SIZE;

    for (final byte[] entry : entries) {
      totalSize += entry.length;
    }

    return super.serializedSize() + totalSize;
  }
}
