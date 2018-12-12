package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class OClusterPageReplaceRecordOperation extends OPageOperationRecord {
  private int    index;
  private byte[] record;
  private int    recordVersion;

  private int    oldRecordVersion;
  private byte[] oldRecord;

  public OClusterPageReplaceRecordOperation() {
  }

  public OClusterPageReplaceRecordOperation(int index, byte[] record, int recordVersion, int oldRecordVersion, byte[] oldRecord) {
    this.index = index;
    this.record = record;
    this.recordVersion = recordVersion;
    this.oldRecordVersion = oldRecordVersion;
    this.oldRecord = oldRecord;
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OClusterPage clusterPage = new OClusterPage(cacheEntry, false);
      clusterPage.replaceRecord(index, record, recordVersion);
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  @Override
  public void undo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(getFileId(), getPageIndex(), false, writeCache, 1, true, null);
    try {
      final OClusterPage clusterPage = new OClusterPage(cacheEntry, false);
      clusterPage.replaceRecord(index, oldRecord, oldRecordVersion);
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
    return WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(oldRecord, 0, content, offset, oldRecord.length);
    offset += oldRecord.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(record.length);
    buffer.put(record);

    buffer.putInt(recordVersion);

    buffer.putInt(oldRecordVersion);

    buffer.putInt(oldRecord.length);
    buffer.put(oldRecord);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];
    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldRecordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int oldRecordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldRecord = new byte[oldRecordLen];
    System.arraycopy(content, offset, oldRecord, 0, oldRecordLen);
    offset += oldRecord.length;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + record.length + oldRecord.length + 4 * OIntegerSerializer.INT_SIZE;
  }
}
