package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.nullbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class OHashIndexNullBucketSetValuePageOperation extends OHashIndexNullBucketPageOperation {
  private byte[] prevValue;
  private int    valueSize;

  public OHashIndexNullBucketSetValuePageOperation() {
  }

  public OHashIndexNullBucketSetValuePageOperation(final byte[] prevValue, final int valueSize) {
    this.prevValue = prevValue;
    this.valueSize = valueSize;
  }

  public final byte[] getPrevValue() {
    return prevValue;
  }

  public final int getValueSize() {
    return valueSize;
  }

  @Override
  protected final void doUndo(final ONullBucket page) {
    if (prevValue == null) {
      page.removeValue(valueSize);
    } else {
      page.setValue(prevValue, valueSize);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    if (prevValue != null) {
      buffer.put((byte) 1);
      serializeByteArray(prevValue, buffer);
    } else {
      buffer.put((byte) 0);
    }
    buffer.putInt(valueSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    final boolean prevValueIsNotNull = buffer.get() > 0;

    if (prevValueIsNotNull) {
      prevValue = deserializeByteArray(buffer);
    } else {
      prevValue = null;
    }

    valueSize = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.HASH_INDEX_NULL_BUCKET_SET_VALUE;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + (prevValue != null ?
        OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + prevValue.length :
        OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE);
  }
}
