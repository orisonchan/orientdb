package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class OClusterPositionMapUndoSetOperation extends OPageOperationRecord {
  private int index;

  private int  recordPageIndex;
  private int  recordPosition;
  private byte flag;

  private int  oldRecordPageIndex;
  private int  oldRecordPosition;
  private byte oldFlag;

  public OClusterPositionMapUndoSetOperation() {
  }

  public OClusterPositionMapUndoSetOperation(int index, int recordPageIndex, int recordPosition, byte flag, int oldRecordPageIndex,
      int oldRecordPosition, byte oldFlag) {
    super();

    this.index = index;
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
    this.flag = flag;
    this.oldRecordPageIndex = oldRecordPageIndex;
    this.oldRecordPosition = oldRecordPosition;
    this.oldFlag = oldFlag;
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
      bucket.undoSet(index, flag, new OClusterPositionMapBucket.PositionEntry(recordPageIndex, recordPosition));
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  @Override
  public void undo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry, false);
      bucket.undoSet(index, oldFlag, new OClusterPositionMapBucket.PositionEntry(oldRecordPageIndex, oldRecordPosition));
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UNDO_SET;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = flag;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecordPageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = oldFlag;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);

    buffer.put(flag);

    buffer.putInt(oldRecordPageIndex);
    buffer.putInt(oldRecordPosition);

    buffer.put(oldFlag);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    flag = content[offset];
    offset++;

    oldRecordPageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldRecordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldFlag = content[offset];
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE;
  }
}
