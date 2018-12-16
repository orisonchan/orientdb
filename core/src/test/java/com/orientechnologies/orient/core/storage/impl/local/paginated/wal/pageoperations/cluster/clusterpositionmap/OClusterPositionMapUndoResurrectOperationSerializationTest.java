package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapUndoResurrectOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 56;
    final int pageIndex = 9;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 1;
    final int recordPageIndex = 12;
    final int recordPosition = 21;
    final int oldRecordPosition = 3;
    final int oldRecordPageIndex = 7;

    OClusterPositionMapUndoResurrectOperation operation = new OClusterPositionMapUndoResurrectOperation(index, recordPageIndex,
        recordPosition, oldRecordPageIndex, oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPositionMapUndoResurrectOperation restoredOperation = new OClusterPositionMapUndoResurrectOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 56;
    final int pageIndex = 9;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 1;
    final int recordPageIndex = 12;
    final int recordPosition = 21;
    final int oldRecordPosition = 3;
    final int oldRecordPageIndex = 7;

    OClusterPositionMapUndoResurrectOperation operation = new OClusterPositionMapUndoResurrectOperation(index, recordPageIndex,
        recordPosition, oldRecordPageIndex, oldRecordPosition);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapUndoResurrectOperation restoredOperation = new OClusterPositionMapUndoResurrectOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
  }
}
