package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public abstract class OPageOperationRecord<T extends ODurablePage> extends OOperationUnitBodyRecord {
  private int  pageIndex;
  private long fileId;

  protected OPageOperationRecord() {
  }

  public final void setPageIndex(final int pageIndex) {
    this.pageIndex = pageIndex;
  }

  public final void setFileId(final long fileId) {
    this.fileId = fileId;
  }

  public final int getPageIndex() {
    return pageIndex;
  }

  public final long getFileId() {
    return fileId;
  }

  @Override
  public final void undo(final OReadCache readCache, final OWriteCache writeCache, final OWriteAheadLog writeAheadLog,
      final OOperationUnitId operationUnitId) throws IOException {
    final OCacheEntry cacheEntry = readCache.loadForWrite(fileId, pageIndex, false, writeCache, 1, true, null);
    try {
      final T page = createPageInstance(cacheEntry);
      doUndo(page);

      final List<OPageOperationRecord> operations = page.getAndClearOperations();

      if (!operations.isEmpty()) {
        OLogSequenceNumber lsn = null;
        for (final OPageOperationRecord pageOperationRecord : operations) {
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

  protected abstract void doUndo(T page);

  @Override
  public final int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.serializeNative(pageIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final ByteBuffer buffer = createNativeByteBuffer(content, offset);
    serializeToByteBuffer(buffer);

    return buffer.position();
  }

  @Override
  public final void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
    buffer.putInt(pageIndex);

    serializeToByteBuffer(buffer);
  }

  @Override
  public final int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageIndex = OIntegerSerializer.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final ByteBuffer buffer = createNativeByteBuffer(content, offset);
    deserializeFromByteBuffer(buffer);

    return buffer.position();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public final boolean isUpdateMasterRecord() {
    return false;
  }

  private static ByteBuffer createNativeByteBuffer(final byte[] content, final int offset) {
    return ByteBuffer.wrap(content, offset, content.length - offset).order(ByteOrder.nativeOrder());
  }

  protected static void serializeByteArray(final byte[] value, final ByteBuffer buffer) {
    if (value != null) {
      buffer.putInt(value.length);
      buffer.put(value);
    } else {
      buffer.putInt(-1);
    }
  }

  protected static byte[] deserializeByteArray(final ByteBuffer buffer) {
    final int len = buffer.getInt();
    if (len >= 0) {
      final byte[] value = new byte[len];
      buffer.get(value);

      return value;
    }

    return null;
  }

  protected abstract void serializeToByteBuffer(ByteBuffer buffer);

  protected abstract void deserializeFromByteBuffer(ByteBuffer buffer);
}
