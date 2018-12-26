package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketUpdateValuePageOperation extends OBonsaiBucketPageOperation {
  private int    index;
  private byte[] prevValue;

  public OBonsaiBucketUpdateValuePageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketUpdateValuePageOperation(final int pageOffset, final int index, final byte[] prevValue) {
    super(pageOffset);
    this.index = index;
    this.prevValue = prevValue;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_UPDATE_VALUE;
  }


  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    final byte keySerializerId = page.getKeySerializerId();
    final OBinarySerializerFactory factory = OBinarySerializerFactory.getInstance();
    final OBinarySerializer keySerializer = factory.getObjectSerializer(keySerializerId);

    //noinspection unchecked
    page.updateValue(index, prevValue, keySerializer);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    serializeByteArray(prevValue, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    prevValue = deserializeByteArray(buffer);
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + prevValue.length;
  }
}
