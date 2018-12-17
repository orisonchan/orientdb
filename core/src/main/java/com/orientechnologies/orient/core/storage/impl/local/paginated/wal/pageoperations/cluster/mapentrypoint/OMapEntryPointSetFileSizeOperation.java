package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.MapEntryPoint;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OMapEntryPointSetFileSizeOperation extends OPageOperationRecord<MapEntryPoint> {
  private int fileSize;
  private int oldFileSize;

  public OMapEntryPointSetFileSizeOperation() {
  }

  public OMapEntryPointSetFileSizeOperation(int fileSize, int oldFileSize) {
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
  protected MapEntryPoint createPageInstance(OCacheEntry cacheEntry) {
    return new MapEntryPoint(cacheEntry, false);
  }

  @Override
  protected void doRedo(MapEntryPoint entryPoint) {
    entryPoint.setFileSize(fileSize);
  }

  @Override
  protected void doUndo(MapEntryPoint entryPoint) {
    entryPoint.setFileSize(oldFileSize);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.MAP_ENTRY_POINT_SET_FILE_SIZE;
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
