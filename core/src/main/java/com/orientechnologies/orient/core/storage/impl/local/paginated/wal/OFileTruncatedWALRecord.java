package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OFileTruncatedWALRecord extends OOperationUnitBodyRecord {
  private long fileId;

  public OFileTruncatedWALRecord() {
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {

  }

  @Override
  public void undo(OReadCache readCache, OWriteCache writeCache, OWriteAheadLog writeAheadLog, OOperationUnitId operationUnitId)
      throws IOException {
    throw new UnsupportedOperationException("File truncation can not be rolled back");
  }

  public OFileTruncatedWALRecord(OOperationUnitId operationUnitId, long fileId) {
    super();
    this.fileId = fileId;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
  }
}
