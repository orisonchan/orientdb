package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.ODirectoryPage;

import java.nio.ByteBuffer;

public final class ODirectoryPageSetNodeLocalDepthPageOperation extends ODirectoryPageOperation {
  private int  localNodeIndex;
  private byte oldDepth;

  public ODirectoryPageSetNodeLocalDepthPageOperation() {
  }

  public ODirectoryPageSetNodeLocalDepthPageOperation(final int localNodeIndex, final byte oldDepth) {
    this.localNodeIndex = localNodeIndex;
    this.oldDepth = oldDepth;
  }

  public final int getLocalNodeIndex() {
    return localNodeIndex;
  }

  final byte getOldDepth() {
    return oldDepth;
  }

  @Override
  protected final void doUndo(final ODirectoryPage page) {
    page.setNodeLocalDepth(localNodeIndex, oldDepth);
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
    return WALRecordTypes.DIRECTORY_PAGE_SET_NODE_LOCAL_DEPTH;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }
}
