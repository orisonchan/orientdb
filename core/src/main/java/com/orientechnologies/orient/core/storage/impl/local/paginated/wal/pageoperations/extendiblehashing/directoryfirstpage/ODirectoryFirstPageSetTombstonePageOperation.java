package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryFirstPage;

import java.nio.ByteBuffer;

public final class ODirectoryFirstPageSetTombstonePageOperation extends ODirectoryFirstPageOperation {
  private int oldTombstone;

  public ODirectoryFirstPageSetTombstonePageOperation() {
  }

  public ODirectoryFirstPageSetTombstonePageOperation(final int oldTombstone) {
    this.oldTombstone = oldTombstone;
  }

  public final int getOldTombstone() {
    return oldTombstone;
  }

  @Override
  protected void doUndo(final ODirectoryFirstPage page) {
    page.setTombstone(oldTombstone);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldTombstone);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldTombstone = buffer.getInt();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.DIRECTORY_FIRST_PAGE_SET_TOMBSTONE;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
