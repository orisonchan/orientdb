package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.filelevelmetadata;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexFileLevelMetadataPage;

import java.nio.ByteBuffer;

public final class OFileLevelMetadataSetRecordsCountPageOperation extends OFileLevelMetadataPageOperation {
  private int oldRecordsCount;

  public OFileLevelMetadataSetRecordsCountPageOperation() {
  }

  public OFileLevelMetadataSetRecordsCountPageOperation(final int oldRecordsCount) {
    this.oldRecordsCount = oldRecordsCount;
  }

  final int getOldRecordsCount() {
    return oldRecordsCount;
  }

  @Override
  protected final void doUndo(final OHashIndexFileLevelMetadataPage page) {
    page.setRecordsCount(oldRecordsCount);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(oldRecordsCount);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    oldRecordsCount = buffer.getInt();
  }

  @Override
  public final int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public final byte getId() {
    return WALRecordTypes.FILE_LEVEL_METADATA_SET_RECORDS_COUNT;
  }
}
