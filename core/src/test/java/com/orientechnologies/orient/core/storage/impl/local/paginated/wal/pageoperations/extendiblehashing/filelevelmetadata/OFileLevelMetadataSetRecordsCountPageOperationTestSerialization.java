package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.filelevelmetadata;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OFileLevelMetadataSetRecordsCountPageOperationTestSerialization {
  @Test
  public void testStreamSerialization() {
    final long fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int oldRecordsCount = 67;

    OFileLevelMetadataSetRecordsCountPageOperation operation = new OFileLevelMetadataSetRecordsCountPageOperation(oldRecordsCount);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OFileLevelMetadataSetRecordsCountPageOperation restoredOperation = new OFileLevelMetadataSetRecordsCountPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldRecordsCount, restoredOperation.getOldRecordsCount());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 456;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int oldRecordsCount = 67;

    OFileLevelMetadataSetRecordsCountPageOperation operation = new OFileLevelMetadataSetRecordsCountPageOperation(oldRecordsCount);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OFileLevelMetadataSetRecordsCountPageOperation restoredOperation = new OFileLevelMetadataSetRecordsCountPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldRecordsCount, restoredOperation.getOldRecordsCount());
  }
}
