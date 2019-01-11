package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings({ "EI_EXPOSE_REP", "EI_EXPOSE_REP2" })
public final class OClusterPageDeleteRecordOperation extends OClusterPageOperation {
  private int    recordVersion;
  private byte[] record;

  public OClusterPageDeleteRecordOperation() {
  }

  public OClusterPageDeleteRecordOperation(final int recordVersion, final byte[] record) {
    this.recordVersion = recordVersion;
    this.record = record;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte[] getRecord() {
    return record;
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.appendRecord(recordVersion, record);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_DELETE_RECORD;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(recordVersion);

    serializeByteArray(record, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    recordVersion = buffer.getInt();
    record = deserializeByteArray(buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + record.length;
  }
}
