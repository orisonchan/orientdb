package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OSBTreeNullBucketSetValuePageOperation extends OSBTreeNullBucketPageOperation {
  private byte[] prevValue;
  private int    valueSize;

  public OSBTreeNullBucketSetValuePageOperation() {
  }

  public OSBTreeNullBucketSetValuePageOperation(final byte[] prevValue, final int valueSize) {
    this.prevValue = prevValue;
    this.valueSize = valueSize;
  }

  public int getValueSize() {
    return valueSize;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_SET_VALUE;
  }

  @Override
  protected void doUndo(final ONullBucket page) {
    if (prevValue != null) {
      page.setValue(prevValue, valueSize);
    } else {
      page.removeValue(valueSize);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(valueSize);
    serializeByteArray(prevValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    valueSize = buffer.getInt();
    prevValue = deserializeByteArray(buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + (prevValue == null ?
        OIntegerSerializer.INT_SIZE :
        (prevValue.length + OIntegerSerializer.INT_SIZE));
  }
}
