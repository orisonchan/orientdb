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
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.MAP_ENTRY_POINT_SET_FILE_SIZE;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(oldFileSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(oldFileSize);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    oldFileSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }
}
