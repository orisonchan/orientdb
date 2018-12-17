package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageAppendRecordOperation extends OPageOperationRecord<OClusterPage> {
  private int    recordVersion;
  private byte[] record;
  private int    index;

  public OClusterPageAppendRecordOperation() {
  }

  public OClusterPageAppendRecordOperation(int recordVersion, byte[] record, int index) {
    this.recordVersion = recordVersion;
    this.record = record;
    this.index = index;
  }

  @Override
  protected OClusterPage createPageInstance(OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, false);
  }

  @Override
  protected void doRedo(OClusterPage clusterPage) {
    clusterPage.appendRecord(recordVersion, record);
  }

  @Override
  protected void doUndo(OClusterPage clusterPage) {
    clusterPage.deleteRecord(index);
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte[] getRecord() {
    return record;
  }

  public int getIndex() {
    return index;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordVersion);

    buffer.putInt(record.length);
    buffer.put(record);

    buffer.putInt(index);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int recordSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordSize];
    System.arraycopy(content, offset, record, 0, record.length);
    offset += record.length;

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + record.length;
  }
}
