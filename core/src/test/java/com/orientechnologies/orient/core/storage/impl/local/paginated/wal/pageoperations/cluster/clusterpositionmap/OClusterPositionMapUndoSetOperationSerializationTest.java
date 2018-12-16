package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapUndoSetOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 12;
    final int pageIndex = 32;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 23;
    final int recordPageIndex = 54;
    final int recordPosition = 34;
    final byte flag = 12;
    final int oldRecordPageIndex = 17;
    final int oldRecordPosition = 43;
    final byte oldFlag = 3;

    OClusterPositionMapUndoSetOperation operation = new OClusterPositionMapUndoSetOperation(index, recordPageIndex, recordPosition,
        flag, oldRecordPageIndex, oldRecordPosition, oldFlag);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPositionMapUndoSetOperation restoredOperation = new OClusterPositionMapUndoSetOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(flag, restoredOperation.getFlag());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldFlag, restoredOperation.getOldFlag());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 12;
    final int pageIndex = 32;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 23;
    final int recordPageIndex = 54;
    final int recordPosition = 34;
    final byte flag = 12;
    final int oldRecordPageIndex = 17;
    final int oldRecordPosition = 43;
    final byte oldFlag = 3;

    OClusterPositionMapUndoSetOperation operation = new OClusterPositionMapUndoSetOperation(index, recordPageIndex, recordPosition,
        flag, oldRecordPageIndex, oldRecordPosition, oldFlag);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapUndoSetOperation restoredOperation = new OClusterPositionMapUndoSetOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(flag, restoredOperation.getFlag());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldFlag, restoredOperation.getOldFlag());
  }

}
