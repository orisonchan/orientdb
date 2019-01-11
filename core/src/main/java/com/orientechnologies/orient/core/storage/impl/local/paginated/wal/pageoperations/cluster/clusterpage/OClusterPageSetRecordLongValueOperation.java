package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OClusterPageSetRecordLongValueOperation extends OClusterPageOperation {
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
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.setRecordLongValue(recordPosition, recordOffset, oldValue);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_SET_RECORD_LONG_VALUE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(recordPosition);
    buffer.putInt(recordOffset);

    buffer.putLong(oldValue);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    recordPosition = buffer.getInt();
    recordOffset = buffer.getInt();
    oldValue = buffer.getLong();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }
}
