package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class OPageOperationRecord<T extends ODurablePage> extends OOperationUnitBodyRecord {
  private int  pageIndex;
  private long fileId;

  public OPageOperationRecord() {
  }

  public final void setPageIndex(int pageIndex) {
    this.pageIndex = pageIndex;
  }

  public final void setFileId(long fileId) {
    this.fileId = fileId;
  }

  public final int getPageIndex() {
    return pageIndex;
  }

  public final long getFileId() {
    return fileId;
  }

  @Override
  public final void redo(OReadCache readCache, OWriteCache writeCache) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, false, writeCache, 1, true, null);
    try {
      final T page = createPageInstance(cacheEntry);
      doRedo(page);
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  @Override
  public final void undo(OReadCache readCache, OWriteCache writeCache, OWriteAheadLog writeAheadLog,
      OOperationUnitId operationUnitId) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, false, writeCache, 1, true, null);
    try {
      final T page = createPageInstance(cacheEntry);
      doUndo(page);

      final List<OPageOperationRecord> operations = page.getOperations();

      if (!operations.isEmpty()) {
        OLogSequenceNumber lsn = null;
        for (OPageOperationRecord pageOperationRecord : operations) {
          pageOperationRecord.setOperationUnitId(operationUnitId);
          lsn = writeAheadLog.log(pageOperationRecord);
        }

        page.setLsn(lsn);
      }
    } finally {
      readCache.releaseFromWrite(cacheEntry, writeCache);
    }
  }

  protected abstract T createPageInstance(OCacheEntry cacheEntry);

  protected abstract void doRedo(T page);

  protected abstract void doUndo(T page);

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
    buffer.putInt(pageIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }
}
