package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class OHashIndexNullBucketRemoveValuePageOperation extends OHashIndexNullBucketPageOperation {
  private byte[] oldValue;

  public OHashIndexNullBucketRemoveValuePageOperation() {
  }

  public OHashIndexNullBucketRemoveValuePageOperation(final byte[] oldValue) {
    this.oldValue = oldValue;
  }

  public final byte[] getOldValue() {
    return oldValue;
  }

  @Override
  protected final void doUndo(final ONullBucket page) {
    page.setValue(oldValue, -1);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    serializeByteArray(oldValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldValue = deserializeByteArray(buffer);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_NULL_BUCKET_REMOVE_VALUE;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + oldValue.length;
  }
}
