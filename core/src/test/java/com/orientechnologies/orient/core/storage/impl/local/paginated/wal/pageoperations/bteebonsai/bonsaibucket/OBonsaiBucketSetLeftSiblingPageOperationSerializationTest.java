package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketSetLeftSiblingPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int prevLeftSiblingPageIndex = 678;
    final int prevLeftSiblingPageOffset = 876;

    final OBonsaiBucketSetLeftSiblingPageOperation operation = new OBonsaiBucketSetLeftSiblingPageOperation(pageOffset,
        prevLeftSiblingPageIndex, prevLeftSiblingPageOffset);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OBonsaiBucketSetLeftSiblingPageOperation restoredOperation = new OBonsaiBucketSetLeftSiblingPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevLeftSiblingPageIndex, restoredOperation.getPrevLeftSiblingPageIndex());
    Assert.assertEquals(prevLeftSiblingPageOffset, restoredOperation.getPrevLeftSiblingPageOffset());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int prevLeftSiblingPageIndex = 678;
    final int prevLeftSiblingPageOffset = 876;

    final OBonsaiBucketSetLeftSiblingPageOperation operation = new OBonsaiBucketSetLeftSiblingPageOperation(pageOffset,
        prevLeftSiblingPageIndex, prevLeftSiblingPageOffset);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OBonsaiBucketSetLeftSiblingPageOperation restoredOperation = new OBonsaiBucketSetLeftSiblingPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevLeftSiblingPageIndex, restoredOperation.getPrevLeftSiblingPageIndex());
    Assert.assertEquals(prevLeftSiblingPageOffset, restoredOperation.getPrevLeftSiblingPageOffset());
  }
}
