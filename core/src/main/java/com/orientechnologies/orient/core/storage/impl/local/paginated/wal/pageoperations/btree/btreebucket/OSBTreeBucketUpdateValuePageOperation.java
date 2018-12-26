package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class OSBTreeBucketUpdateValuePageOperation extends OPageOperationRecord<OSBTreeBucket> {
  private int     index;
  private byte[]  prevValue;
  private boolean isEncrypted;
  private byte    keySerializerId;

  public OSBTreeBucketUpdateValuePageOperation() {
  }

  public OSBTreeBucketUpdateValuePageOperation(final int index, final byte[] prevValue, final byte keySerializerId,
      final boolean isEncrypted) {
    this.index = index;
    this.prevValue = prevValue;
    this.keySerializerId = keySerializerId;
    this.isEncrypted = isEncrypted;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_UPDATE_VALUE;
  }

  @Override
  protected OSBTreeBucket createPageInstance(final OCacheEntry cacheEntry) {
    return new OSBTreeBucket(cacheEntry);
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    //noinspection unchecked
    page.updateValue(index, prevValue, factory.getObjectSerializer(keySerializerId), isEncrypted);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(prevValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(prevValue, 0, content, offset, prevValue.length);
    offset += prevValue.length;

    content[offset] = keySerializerId;
    offset++;

    content[offset] = isEncrypted ? (byte) 1 : 0;
    offset++;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(prevValue.length);
    buffer.put(prevValue);

    buffer.put(keySerializerId);

    buffer.put(isEncrypted ? (byte) 1 : 0);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int prevValLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevValue = new byte[prevValLen];
    System.arraycopy(content, offset, prevValue, 0, prevValLen);
    offset += prevValLen;

    keySerializerId = content[offset];
    offset++;

    isEncrypted = content[offset] == 1;
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + prevValue.length + 2 * OByteSerializer.BYTE_SIZE;
  }
}
