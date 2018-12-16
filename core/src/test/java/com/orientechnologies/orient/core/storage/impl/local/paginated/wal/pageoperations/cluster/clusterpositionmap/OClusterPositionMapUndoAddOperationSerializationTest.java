package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapUndoAddOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 34;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldPageIndex = 54;
    final int oldRecordPosition = 45;

    OClusterPositionMapUndoAddOperation operation = new OClusterPositionMapUndoAddOperation(oldPageIndex, oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPositionMapUndoAddOperation restoredOperation = new OClusterPositionMapUndoAddOperation();
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
    final int fileId = 34;
    final int pageIndex = 12;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int oldPageIndex = 54;
    final int oldRecordPosition = 45;

    OClusterPositionMapUndoAddOperation operation = new OClusterPositionMapUndoAddOperation(oldPageIndex, oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapUndoAddOperation restoredOperation = new OClusterPositionMapUndoAddOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldPageIndex, restoredOperation.getOldPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
  }
}
