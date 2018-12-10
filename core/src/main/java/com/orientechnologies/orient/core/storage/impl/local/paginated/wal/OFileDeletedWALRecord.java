package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class OFileDeletedWALRecord extends OOperationUnitBodyRecord {
  private long fileId;

  OFileDeletedWALRecord() {
  }

  @Override
  public void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    try {
      readCache.deleteFile(fileId, writeCache);
    } catch (IOException e) {
      throw OException.wrapException(new OStorageException("Can not delete file with id " + fileId), e);
    }
  }

  @Override
  public void undo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    throw new OStorageException("File deletion can not be rolled back");
  }

  public OFileDeletedWALRecord(long fileId) {
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
    return WALRecordTypes.FILE_DELETED_WAL_RECORD;
  }
}
