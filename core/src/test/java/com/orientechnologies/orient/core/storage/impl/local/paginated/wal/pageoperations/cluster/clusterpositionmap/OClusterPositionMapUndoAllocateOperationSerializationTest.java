package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapUndoAllocateOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 12;
    final int pageIndex = 21;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldPageIndex = 32;
    final int oldRecordPosition = 23;

    OClusterPositionMapUndoAllocateOperation operation = new OClusterPositionMapUndoAllocateOperation(oldPageIndex,
        oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPositionMapUndoAllocateOperation restoredOperation = new OClusterPositionMapUndoAllocateOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldPageIndex, restoredOperation.getOldPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 12;
    final int pageIndex = 21;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldPageIndex = 32;
    final int oldRecordPosition = 23;

    OClusterPositionMapUndoAllocateOperation operation = new OClusterPositionMapUndoAllocateOperation(oldPageIndex,
        oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapUndoAllocateOperation restoredOperation = new OClusterPositionMapUndoAllocateOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldPageIndex, restoredOperation.getOldPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
  }
}
