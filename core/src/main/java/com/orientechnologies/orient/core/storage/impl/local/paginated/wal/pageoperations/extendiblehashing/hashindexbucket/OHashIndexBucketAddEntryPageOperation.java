package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;

import java.nio.ByteBuffer;

public final class OHashIndexBucketAddEntryPageOperation extends OHashIndexBucketPageOperation {
  private int index;
  private int keySize;
  private int valueSize;

  public OHashIndexBucketAddEntryPageOperation() {
  }

  public OHashIndexBucketAddEntryPageOperation(final int index, final int keySize, final int valueSize) {
    this.index = index;
    this.keySize = keySize;
    this.valueSize = valueSize;
  }

  @Override
  protected final void doUndo(final OHashIndexBucket page) {
    page.deleteEntry(index, keySize, valueSize);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putInt(keySize);
    buffer.putInt(valueSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    keySize = buffer.getInt();
    valueSize = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_BUCKET_ADD_ENTRY;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }
}
