package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketSetFreeListPointerPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 54;
    final int freeListPageIndex = 123;
    final int freeListPageOffset = 321;

    OBonsaiBucketSetFreeListPointerPageOperation operation = new OBonsaiBucketSetFreeListPointerPageOperation(pageOffset,
        freeListPageIndex, freeListPageOffset);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OBonsaiBucketSetFreeListPointerPageOperation restoredOperation = new OBonsaiBucketSetFreeListPointerPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(freeListPageIndex, restoredOperation.getPrevPageIndex());
    Assert.assertEquals(freeListPageOffset, restoredOperation.getPrevPageOffset());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 54;
    final int freeListPageIndex = 123;
    final int freeListPageOffset = 321;

    OBonsaiBucketSetFreeListPointerPageOperation operation = new OBonsaiBucketSetFreeListPointerPageOperation(pageOffset,
        freeListPageIndex, freeListPageOffset);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OBonsaiBucketSetFreeListPointerPageOperation restoredOperation = new OBonsaiBucketSetFreeListPointerPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(freeListPageIndex, restoredOperation.getPrevPageIndex());
    Assert.assertEquals(freeListPageOffset, restoredOperation.getPrevPageOffset());
  }
}
