package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OSBTreeNullBucketRemoveValuePageOperation extends OPageOperationRecord<ONullBucket> {
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
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_REMOVE_VALUE;
  }

  @Override
  protected ONullBucket createPageInstance(final OCacheEntry cacheEntry) {
    //noinspection unchecked
    return new ONullBucket(cacheEntry, null, false);
  }

  @Override
  protected void doUndo(final ONullBucket page) {
    page.setValue(previousValue);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(previousValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(previousValue, 0, content, offset, previousValue.length);
    offset += previousValue.length;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(previousValue.length);
    buffer.put(previousValue);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int prevValueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    previousValue = new byte[prevValueLen];

    System.arraycopy(content, offset, previousValue, 0, prevValueLen);
    offset += prevValueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + previousValue.length;
  }
}
