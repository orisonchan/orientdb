package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

import java.nio.ByteBuffer;

public final class OBonsaiSysBucketSetFreeSpacePointerPageOperation extends OBonsaiSysBucketPageOperation {
  private int prevPointerPageIndex;
  private int prevPointerPageOffset;

  public OBonsaiSysBucketSetFreeSpacePointerPageOperation() {
  }

  public OBonsaiSysBucketSetFreeSpacePointerPageOperation(final int prevPointerPageIndex, final int prevPointerPageOffset) {
    this.prevPointerPageIndex = prevPointerPageIndex;
    this.prevPointerPageOffset = prevPointerPageOffset;
  }

  @Override
  protected final void doUndo(final OSysBucket page) {
    page.setFreeSpacePointer(new OBonsaiBucketPointer(prevPointerPageIndex, prevPointerPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(prevPointerPageIndex);
    buffer.putInt(prevPointerPageOffset);

  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    prevPointerPageIndex = buffer.getInt();
    prevPointerPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_SPACE_POINTER;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
