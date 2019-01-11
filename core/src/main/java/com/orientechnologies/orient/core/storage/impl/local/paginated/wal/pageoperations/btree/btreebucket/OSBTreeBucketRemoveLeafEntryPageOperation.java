package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class OSBTreeBucketRemoveLeafEntryPageOperation extends OSBTreeBucketPageOperation {
  private int    entryIndex;
  private byte[] rawKey;
  private byte[] rawValue;

  public OSBTreeBucketRemoveLeafEntryPageOperation() {
  }

  public OSBTreeBucketRemoveLeafEntryPageOperation(final int entryIndex, final byte[] rawKey, final byte[] rawValue) {
    this.entryIndex = entryIndex;
    this.rawKey = rawKey;
    this.rawValue = rawValue;
  }

  public int getEntryIndex() {
    return entryIndex;
  }

  public byte[] getRawKey() {
    return rawKey;
  }

  public byte[] getRawValue() {
    return rawValue;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    page.insertLeafKeyValue(entryIndex, rawKey, rawValue);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_REMOVE_LEAF_ENTRY;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(entryIndex);

    serializeByteArray(rawKey, buffer);
    serializeByteArray(rawValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    entryIndex = buffer.getInt();

    rawKey = deserializeByteArray(buffer);
    rawValue = deserializeByteArray(buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + rawKey.length + rawValue.length;
  }
}
