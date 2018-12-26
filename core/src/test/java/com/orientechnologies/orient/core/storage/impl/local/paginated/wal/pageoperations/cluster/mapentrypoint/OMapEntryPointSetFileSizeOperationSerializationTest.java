package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.mapentrypoint;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OMapEntryPointSetFileSizeOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 45;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldFileSize = 32;

    OMapEntryPointSetFileSizeOperation operation = new OMapEntryPointSetFileSizeOperation(oldFileSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OMapEntryPointSetFileSizeOperation restoredOperation = new OMapEntryPointSetFileSizeOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldFileSize, restoredOperation.getOldFileSize());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 45;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldFileSize = 32;

    OMapEntryPointSetFileSizeOperation operation = new OMapEntryPointSetFileSizeOperation(oldFileSize);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OMapEntryPointSetFileSizeOperation restoredOperation = new OMapEntryPointSetFileSizeOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldFileSize, restoredOperation.getOldFileSize());
  }
}
