package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OSBTreeNullBucketRemoveValuePageOperation extends OSBTreeNullBucketPageOperation {
  private byte[] previousValue;

  public OSBTreeNullBucketRemoveValuePageOperation() {
  }

  public OSBTreeNullBucketRemoveValuePageOperation(final byte[] previousValue) {
    this.previousValue = previousValue;
  }

  public byte[] getPreviousValue() {
    return previousValue;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_REMOVE_VALUE;
  }

  @Override
  protected void doUndo(final ONullBucket page) {
    page.setValue(previousValue, -1);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + previousValue.length;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    serializeByteArray(previousValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    previousValue = deserializeByteArray(buffer);
  }
}
