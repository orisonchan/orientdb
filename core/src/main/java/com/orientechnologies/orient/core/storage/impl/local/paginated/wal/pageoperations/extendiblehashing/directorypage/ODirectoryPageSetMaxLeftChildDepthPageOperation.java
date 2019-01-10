package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryPage;

import java.nio.ByteBuffer;

public final class ODirectoryPageSetMaxLeftChildDepthPageOperation extends ODirectoryPageOperation {
  private int  localNodeIndex;
  private byte oldDepth;

  public ODirectoryPageSetMaxLeftChildDepthPageOperation() {
  }

  public ODirectoryPageSetMaxLeftChildDepthPageOperation(final int localNodeIndex, final byte oldDepth) {
    this.oldDepth = oldDepth;
    this.localNodeIndex = localNodeIndex;
  }

  public final int getLocalNodeIndex() {
    return localNodeIndex;
  }

  final byte getOldDepth() {
    return oldDepth;
  }

  @Override
  protected final void doUndo(final ODirectoryPage page) {
    page.setMaxLeftChildDepth(localNodeIndex, oldDepth);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(localNodeIndex);
    buffer.put(oldDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    localNodeIndex = buffer.getInt();
    oldDepth = buffer.get();
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.DIRECTORY_PAGE_SET_MAX_LEFT_CHILD_DEPTH;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }
}
