package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapSetOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 23;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 12;
    final int recordPosition = 45;
    final int recordPageIndex = 121;

    final int oldRecordPageIndex = 212;
    final int oldRecordPosition = 54;
    final byte oldFlag = 23;

    OClusterPositionMapSetOperation operation = new OClusterPositionMapSetOperation(index, recordPosition, recordPageIndex,
        oldRecordPageIndex, oldRecordPosition, oldFlag);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OClusterPositionMapSetOperation restoredOperation = new OClusterPositionMapSetOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldFlag, restoredOperation.getOldFlag());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 23;
    final int pageIndex = 34;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int index = 12;
    final int recordPosition = 45;
    final int recordPageIndex = 121;

    final int oldRecordPageIndex = 212;
    final int oldRecordPosition = 54;
    final byte oldFlag = 23;

    OClusterPositionMapSetOperation operation = new OClusterPositionMapSetOperation(index, recordPosition, recordPageIndex,
        oldRecordPageIndex, oldRecordPosition, oldFlag);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapSetOperation restoredOperation = new OClusterPositionMapSetOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertEquals(recordPosition, restoredOperation.getRecordPosition());
    Assert.assertEquals(recordPageIndex, restoredOperation.getRecordPageIndex());
    Assert.assertEquals(oldRecordPageIndex, restoredOperation.getOldRecordPageIndex());
    Assert.assertEquals(oldRecordPosition, restoredOperation.getOldRecordPosition());
    Assert.assertEquals(oldFlag, restoredOperation.getOldFlag());
  }
}
