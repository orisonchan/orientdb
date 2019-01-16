package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.nio.ByteBuffer;

final class OFileTruncatedWALRecord extends OOperationUnitBodyRecord {
  private long fileId;

  OFileTruncatedWALRecord() {
  }


  @Override
  public void undo(final OReadCache readCache, final OWriteCache writeCache, final OWriteAheadLog writeAheadLog,
      final OOperationUnitId operationUnitId) {
    throw new UnsupportedOperationException("File truncation can not be rolled back");
  }

  public OFileTruncatedWALRecord(final OOperationUnitId operationUnitId, final long fileId) {
    super();
    this.fileId = fileId;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
  }
}
