package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaisysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSysBucket;

import java.nio.ByteBuffer;

public final class OBonsaiSysBucketSetFreeSpacePointerPageOperation extends OBonsaiSysBucketPageOperation {
  private int pointerPageIndex;
  private int pointerPageOffset;

  private int prevPointerPageIndex;
  private int prevPointerPageOffset;

  public OBonsaiSysBucketSetFreeSpacePointerPageOperation() {
  }

  public OBonsaiSysBucketSetFreeSpacePointerPageOperation(final int pointerPageIndex, final int pointerPageOffset,
      final int prevPointerPageIndex, final int prevPointerPageOffset) {
    this.pointerPageIndex = pointerPageIndex;
    this.pointerPageOffset = pointerPageOffset;
    this.prevPointerPageIndex = prevPointerPageIndex;
    this.prevPointerPageOffset = prevPointerPageOffset;
  }

  @Override
  protected final void doRedo(final OSysBucket page) {
    page.setFreeSpacePointer(new OBonsaiBucketPointer(pointerPageIndex, pointerPageOffset));
  }

  @Override
  protected final void doUndo(final OSysBucket page) {
    page.setFreeSpacePointer(new OBonsaiBucketPointer(prevPointerPageIndex, prevPointerPageOffset));
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(pointerPageIndex);
    buffer.putInt(pointerPageOffset);

    buffer.putInt(prevPointerPageIndex);
    buffer.putInt(prevPointerPageOffset);

  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    pointerPageIndex = buffer.getInt();
    pointerPageOffset = buffer.getInt();

    prevPointerPageIndex = buffer.getInt();
    prevPointerPageOffset = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_SYS_BUCKET_SET_FREE_SPACE_POINTER;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE;
  }
}
