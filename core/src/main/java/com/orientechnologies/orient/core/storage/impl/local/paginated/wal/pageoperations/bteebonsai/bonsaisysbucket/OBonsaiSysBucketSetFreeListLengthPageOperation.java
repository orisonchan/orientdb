package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

import java.nio.ByteBuffer;

public final class OBonsaiSysBucketSetFreeListLengthPageOperation extends OBonsaiSysBucketPageOperation {
  private int prevLength;

  public OBonsaiSysBucketSetFreeListLengthPageOperation() {
  }

  public OBonsaiSysBucketSetFreeListLengthPageOperation(final int prevLength) {
    this.prevLength = prevLength;
  }

  int getPrevLength() {
    return prevLength;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevLength);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevLength = buffer.getInt();
  }

  @Override
  protected final void doUndo(final OSysBucket page) {
    page.setFreeListLength(prevLength);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_LIST_LENGTH;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
