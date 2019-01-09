package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class OHashIndexBucketUpdateEntryPageOperation extends OHashIndexBucketPageOperation {
  private int    index;
  private int    valueSize;
  private int    keySize;
  private byte[] oldValue;

  public OHashIndexBucketUpdateEntryPageOperation() {
  }

  public OHashIndexBucketUpdateEntryPageOperation(final int index, final int keySize, final int valueSize, final byte[] oldValue) {
    this.index = index;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.oldValue = oldValue;
  }

  @Override
  protected final void doUndo(final OHashIndexBucket page) {
    page.updateEntry(index, oldValue, valueSize, keySize);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putInt(valueSize);
    buffer.putInt(keySize);

    serializeByteArray(oldValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    valueSize = buffer.getInt();
    keySize = buffer.getInt();

    oldValue = deserializeByteArray(buffer);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_BUCKET_UPDATE_ENTRY;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE + oldValue.length;
  }
}
