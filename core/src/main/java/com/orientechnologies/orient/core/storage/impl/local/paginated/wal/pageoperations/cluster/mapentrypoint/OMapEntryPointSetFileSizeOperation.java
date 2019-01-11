package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.MapEntryPoint;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperationRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public final class OMapEntryPointSetFileSizeOperation extends OPageOperationRecord<MapEntryPoint> {
  private int oldFileSize;

  public OMapEntryPointSetFileSizeOperation() {
  }

  public OMapEntryPointSetFileSizeOperation(final int oldFileSize) {
    this.oldFileSize = oldFileSize;
  }

  final int getOldFileSize() {
    return oldFileSize;
  }

  @Override
  protected MapEntryPoint createPageInstance(final OCacheEntry cacheEntry) {
    return new MapEntryPoint(cacheEntry);
  }

  @Override
  protected void doUndo(final MapEntryPoint entryPoint) {
    entryPoint.setFileSize(oldFileSize);
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.MAP_ENTRY_POINT_SET_FILE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldFileSize);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldFileSize = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
