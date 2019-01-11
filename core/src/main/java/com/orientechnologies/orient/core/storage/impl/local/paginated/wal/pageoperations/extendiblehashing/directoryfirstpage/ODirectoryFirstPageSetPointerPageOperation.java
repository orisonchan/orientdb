package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryFirstPage;

import java.nio.ByteBuffer;

public final class ODirectoryFirstPageSetPointerPageOperation extends ODirectoryFirstPageOperation {
  private int localNodeIndex;
  private int index;

  private long oldPointer;

  public ODirectoryFirstPageSetPointerPageOperation() {
  }

  public ODirectoryFirstPageSetPointerPageOperation(final int localNodeIndex, final int index, final long oldPointer) {
    this.localNodeIndex = localNodeIndex;
    this.index = index;
    this.oldPointer = oldPointer;
  }

  public final int getLocalNodeIndex() {
    return localNodeIndex;
  }

  public final int getIndex() {
    return index;
  }

  final long getOldPointer() {
    return oldPointer;
  }

  @Override
  protected final void doUndo(final ODirectoryFirstPage page) {
    page.setPointer(localNodeIndex, index, oldPointer);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(localNodeIndex);
    buffer.putInt(index);
    buffer.putLong(oldPointer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    localNodeIndex = buffer.getInt();
    index = buffer.getInt();
    oldPointer = buffer.getLong();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_POINTER;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }
}
