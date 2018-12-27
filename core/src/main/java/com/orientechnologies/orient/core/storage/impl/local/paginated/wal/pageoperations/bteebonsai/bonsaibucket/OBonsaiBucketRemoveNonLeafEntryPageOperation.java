package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings("EI_EXPOSE_REP")
public final class OBonsaiBucketRemoveNonLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int    entryIndex;
  private byte[] key;

  private int leftPageIndex;
  private int leftPageOffset;

  private int rightPageIndex;
  private int rightPageOffset;

  public OBonsaiBucketRemoveNonLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketRemoveNonLeafEntryPageOperation(final int pageOffset, final int entryIndex, final byte[] key,
      final int leftPageIndex, final int leftPageOffset, final int rightPageIndex, final int rightPageOffset) {
    super(pageOffset);

    this.entryIndex = entryIndex;
    this.key = key;
    this.leftPageIndex = leftPageIndex;
    this.leftPageOffset = leftPageOffset;
    this.rightPageIndex = rightPageIndex;
    this.rightPageOffset = rightPageOffset;
  }

  public int getEntryIndex() {
    return entryIndex;
  }

  public byte[] getKey() {
    return key;
  }

  int getLeftPageIndex() {
    return leftPageIndex;
  }

  int getLeftPageOffset() {
    return leftPageOffset;
  }

  int getRightPageIndex() {
    return rightPageIndex;
  }

  int getRightPageOffset() {
    return rightPageOffset;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_REMOVE_NON_LEAF_ENTRY;
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.insertNonLeafEntry(entryIndex, key, new OBonsaiBucketPointer(leftPageIndex, leftPageOffset),
        new OBonsaiBucketPointer(rightPageIndex, rightPageOffset), leftPageIndex >= 0 || rightPageIndex >= 0);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(entryIndex);

    serializeByteArray(key, buffer);

    buffer.putInt(leftPageIndex);
    buffer.putInt(leftPageOffset);

    buffer.putInt(rightPageIndex);
    buffer.putInt(rightPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    entryIndex = buffer.getInt();

    key = deserializeByteArray(buffer);

    leftPageIndex = buffer.getInt();
    leftPageOffset = buffer.getInt();

    rightPageIndex = buffer.getInt();
    rightPageOffset = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 6 * OIntegerSerializer.INT_SIZE + key.length;
  }
}
