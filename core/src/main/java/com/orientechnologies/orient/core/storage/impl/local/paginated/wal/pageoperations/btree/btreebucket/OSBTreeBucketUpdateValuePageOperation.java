package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP2", "EI_EXPOSE_REP" })
public final class OSBTreeBucketUpdateValuePageOperation extends OSBTreeBucketPageOperation {
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
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_UPDATE_VALUE;
  }

  @Override
  protected void doUndo(final OSBTreeBucket page) {
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    //noinspection unchecked
    page.updateValue(index, prevValue, factory.getObjectSerializer(keySerializerId), isEncrypted);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    serializeByteArray(prevValue, buffer);

    buffer.put(keySerializerId);

    buffer.put(isEncrypted ? (byte) 1 : 0);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    prevValue = deserializeByteArray(buffer);
    keySerializerId = buffer.get();
    isEncrypted = buffer.get() > 0;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + prevValue.length + 2 * OByteSerializer.BYTE_SIZE;
  }
}
