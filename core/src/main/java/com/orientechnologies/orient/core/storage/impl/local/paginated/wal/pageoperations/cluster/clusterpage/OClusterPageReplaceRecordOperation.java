package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class OClusterPageReplaceRecordOperation extends OClusterPageOperation {
  private int index;

  private int    oldRecordVersion;
  private byte[] oldRecord;

  public OClusterPageReplaceRecordOperation() {
  }

  public OClusterPageReplaceRecordOperation(final int index, final int oldRecordVersion, final byte[] oldRecord) {
    this.index = index;
    this.oldRecordVersion = oldRecordVersion;
    this.oldRecord = oldRecord;
  }

  public int getIndex() {
    return index;
  }

  int getOldRecordVersion() {
    return oldRecordVersion;
  }

  byte[] getOldRecord() {
    return oldRecord;
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.replaceRecord(index, oldRecord, oldRecordVersion);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(oldRecordVersion);

    serializeByteArray(oldRecord, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    index = buffer.getInt();
    oldRecordVersion = buffer.getInt();
    oldRecord = deserializeByteArray(buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + oldRecord.length + 3 * OIntegerSerializer.INT_SIZE;
  }
}
