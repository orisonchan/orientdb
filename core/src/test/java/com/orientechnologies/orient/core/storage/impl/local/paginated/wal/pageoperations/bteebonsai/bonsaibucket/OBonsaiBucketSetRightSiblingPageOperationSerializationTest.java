package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketSetRightSiblingPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final int pageOffset = 678;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int prevRightSiblingPageIndex = 980;
    final int prevRightSiblingPageOffset = 890;

    final OBonsaiBucketSetRightSiblingPageOperation operation = new OBonsaiBucketSetRightSiblingPageOperation(pageOffset,
        prevRightSiblingPageIndex, prevRightSiblingPageOffset);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OBonsaiBucketSetRightSiblingPageOperation restoredOperation = new OBonsaiBucketSetRightSiblingPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevRightSiblingPageIndex, restoredOperation.getPrevRightSiblingPageIndex());
    Assert.assertEquals(prevRightSiblingPageOffset, restoredOperation.getPrevRightSiblingOffset());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final int pageOffset = 678;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int prevRightSiblingPageIndex = 980;
    final int prevRightSiblingPageOffset = 890;

    final OBonsaiBucketSetRightSiblingPageOperation operation = new OBonsaiBucketSetRightSiblingPageOperation(pageOffset,
        prevRightSiblingPageIndex, prevRightSiblingPageOffset);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OBonsaiBucketSetRightSiblingPageOperation restoredOperation = new OBonsaiBucketSetRightSiblingPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(prevRightSiblingPageIndex, restoredOperation.getPrevRightSiblingPageIndex());
    Assert.assertEquals(prevRightSiblingPageOffset, restoredOperation.getPrevRightSiblingOffset());
  }
}
