package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiBucket;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

public final class OBonsaiBucketInsertNonLeafEntryPageOperation extends OBonsaiBucketPageOperation {
  private int    index;
  private byte[] serializedKey;

  private int leftPageIndex;
  private int leftPageOffset;
  private int rightPageIndex;
  private int rightPageOffset;

  private int prevChildPageIndex;
  private int prevChildPageOffset;

  public OBonsaiBucketInsertNonLeafEntryPageOperation() {
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public OBonsaiBucketInsertNonLeafEntryPageOperation(final int pageOffset, final int index, final byte[] serializedKey,
      final int leftPageIndex, final int leftPageOffset, final int rightPageIndex, final int rightPageOffset,
      final int prevChildPageIndex, final int prevChildPageOffset) {
    super(pageOffset);
    this.index = index;
    this.serializedKey = serializedKey;
    this.leftPageIndex = leftPageIndex;
    this.leftPageOffset = leftPageOffset;
    this.rightPageIndex = rightPageIndex;
    this.rightPageOffset = rightPageOffset;

    this.prevChildPageIndex = prevChildPageIndex;
    this.prevChildPageOffset = prevChildPageOffset;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_INSERT_NON_LEAF_ENTRY;
  }

  @Override
  protected final void doRedo(final OSBTreeBonsaiBucket page) {
    page.insertNonLeafEntry(index, serializedKey, new OBonsaiBucketPointer(leftPageIndex, leftPageOffset),
        new OBonsaiBucketPointer(rightPageIndex, rightPageOffset), leftPageIndex >= 0 || rightPageIndex >= 0);
  }

  @Override
  protected final void doUndo(final OSBTreeBonsaiBucket page) {
    page.removeNonLeafEntry(index, serializedKey, prevChildPageIndex, prevChildPageOffset);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);
    serializeByteArray(serializedKey, buffer);

    buffer.putInt(leftPageIndex);
    buffer.putInt(leftPageOffset);

    buffer.putInt(rightPageIndex);
    buffer.putInt(rightPageOffset);

    buffer.putInt(prevChildPageIndex);
    buffer.putInt(prevChildPageOffset);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();

    serializedKey = deserializeByteArray(buffer);

    leftPageIndex = buffer.getInt();
    leftPageOffset = buffer.getInt();

    rightPageIndex = buffer.getInt();
    rightPageOffset = buffer.getInt();

    prevChildPageIndex = buffer.getInt();
    prevChildPageOffset = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 8 * OIntegerSerializer.INT_SIZE + serializedKey.length;
  }
}
