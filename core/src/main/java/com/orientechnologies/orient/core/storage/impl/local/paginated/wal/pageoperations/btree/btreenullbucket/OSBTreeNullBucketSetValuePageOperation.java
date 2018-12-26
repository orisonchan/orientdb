package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreenullbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.ONullBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OSBTreeNullBucketSetValuePageOperation extends OPageOperationRecord<ONullBucket> {
  private byte[] prevValue;

  public OSBTreeNullBucketSetValuePageOperation() {
  }

  public OSBTreeNullBucketSetValuePageOperation(final byte[] prevValue) {
    this.prevValue = prevValue;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_NULL_BUCKET_SET_VALUE;
  }

  @Override
  protected ONullBucket createPageInstance(final OCacheEntry cacheEntry) {
    //noinspection unchecked
    return new ONullBucket(cacheEntry, null, false);
  }

  @Override
  protected void doUndo(final ONullBucket page) {
    if (prevValue != null) {
      page.setValue(prevValue);
    } else {
      page.removeValue();
    }
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    if (prevValue == null) {
      offset++;
    } else {
      content[offset] = 1;
      offset++;

      OIntegerSerializer.INSTANCE.serializeNative(prevValue.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(prevValue, 0, content, offset, prevValue.length);
      offset += prevValue.length;
    }

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    if (prevValue == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);

      buffer.putInt(prevValue.length);
      buffer.put(prevValue);
    }
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    if (content[offset] == 0) {
      offset++;
    } else {
      offset++;

      final int prevValLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      prevValue = new byte[prevValLen];
      System.arraycopy(content, offset, prevValue, 0, prevValLen);
      offset += prevValLen;
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + (prevValue == null ?
        OByteSerializer.BYTE_SIZE :
        (prevValue.length + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE));
  }
}
