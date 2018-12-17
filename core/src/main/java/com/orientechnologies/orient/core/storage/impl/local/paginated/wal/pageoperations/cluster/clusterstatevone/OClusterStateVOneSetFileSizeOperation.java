package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstatevone;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterStateVOneSetFileSizeOperation extends OPageOperationRecord<OPaginatedClusterStateV1> {
  private int fileSize;
  private int oldFileSize;

  public OClusterStateVOneSetFileSizeOperation() {
  }

  public OClusterStateVOneSetFileSizeOperation(int fileSize, int oldFileSize) {
    this.fileSize = fileSize;
    this.oldFileSize = oldFileSize;
  }

  public int getFileSize() {
    return fileSize;
  }

  int getOldFileSize() {
    return oldFileSize;
  }

  @Override
  protected OPaginatedClusterStateV1 createPageInstance(OCacheEntry cacheEntry) {
    return new OPaginatedClusterStateV1(cacheEntry, false);
  }

  @Override
  protected void doRedo(OPaginatedClusterStateV1 clusterState) {
    clusterState.setFileSize(fileSize);
  }

  @Override
  protected void doUndo(OPaginatedClusterStateV1 clusterState) {
    clusterState.setFileSize(oldFileSize);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_V_ONE_SET_FILE_SIZE;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(fileSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldFileSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(fileSize);
    buffer.putInt(oldFileSize);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldFileSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }
}
