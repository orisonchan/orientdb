package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetRecordLongValueOperation extends OPageOperationRecord<OClusterPage> {
  private int recordPosition;
  private int recordOffset;

  private long oldValue;

  public OClusterPageSetRecordLongValueOperation() {
  }

  public OClusterPageSetRecordLongValueOperation(final int recordPosition, final int recordOffset, final long oldValue) {
    this.recordPosition = recordPosition;
    this.recordOffset = recordOffset;
    this.oldValue = oldValue;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  int getRecordOffset() {
    return recordOffset;
  }

  public long getOldValue() {
    return oldValue;
  }

  @Override
  protected OClusterPage createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, false);
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.setRecordLongValue(recordPosition, recordOffset, oldValue);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_SET_RECORD_LONG_VALUE;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordPosition, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(oldValue, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordPosition);
    buffer.putInt(recordOffset);

    buffer.putLong(oldValue);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordPosition = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldValue = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }
}
