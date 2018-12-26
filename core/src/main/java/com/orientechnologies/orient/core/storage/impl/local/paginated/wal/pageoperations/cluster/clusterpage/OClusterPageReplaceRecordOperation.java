package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.ByteBuffer;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public final class OClusterPageReplaceRecordOperation extends OPageOperationRecord<OClusterPage> {
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
  protected OClusterPage createPageInstance(final OCacheEntry cacheEntry) {
    return new OClusterPage(cacheEntry, false);
  }

  @Override
  protected void doUndo(final OClusterPage clusterPage) {
    clusterPage.replaceRecord(index, oldRecord, oldRecordVersion);
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
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecord.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(oldRecord, 0, content, offset, oldRecord.length);
    offset += oldRecord.length;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(index);

    buffer.putInt(oldRecordVersion);

    buffer.putInt(oldRecord.length);
    buffer.put(oldRecord);
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
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
    return super.serializedSize() + oldRecord.length + 3 * OIntegerSerializer.INT_SIZE;
  }
}
