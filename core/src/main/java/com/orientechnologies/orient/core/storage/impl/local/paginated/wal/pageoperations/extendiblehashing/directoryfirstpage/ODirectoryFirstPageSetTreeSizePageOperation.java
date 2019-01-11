package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryFirstPage;

import java.nio.ByteBuffer;

public final class ODirectoryFirstPageSetTreeSizePageOperation extends ODirectoryFirstPageOperation {
  private int oldTreeSize;

  public ODirectoryFirstPageSetTreeSizePageOperation() {
  }

  public ODirectoryFirstPageSetTreeSizePageOperation(final int oldTreeSize) {
    this.oldTreeSize = oldTreeSize;
  }

  public final int getOldTreeSize() {
    return oldTreeSize;
  }

  @Override
  protected void doUndo(final ODirectoryFirstPage page) {
    page.setTreeSize(oldTreeSize);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldTreeSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldTreeSize = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_TREE_SIZE;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
