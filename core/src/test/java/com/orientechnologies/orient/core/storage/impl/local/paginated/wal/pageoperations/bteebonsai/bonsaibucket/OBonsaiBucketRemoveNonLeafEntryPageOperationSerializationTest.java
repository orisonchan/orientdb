package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketRemoveNonLeafEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int pageOffset = 12;

    final int entryIndex = 45;
    final byte[] key = new byte[12];

    final int leftPageIndex = 43;
    final int leftPageOffset = 430;
    final int rightPageIndex = 34;
    final int rightPageOffset = 340;

    OBonsaiBucketRemoveNonLeafEntryPageOperation operation = new OBonsaiBucketRemoveNonLeafEntryPageOperation(pageOffset,
        entryIndex, key, leftPageIndex, leftPageOffset, rightPageIndex, rightPageOffset);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OBonsaiBucketRemoveNonLeafEntryPageOperation restoredOperation = new OBonsaiBucketRemoveNonLeafEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(entryIndex, restoredOperation.getEntryIndex());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertEquals(leftPageIndex, restoredOperation.getLeftPageIndex());
    Assert.assertEquals(leftPageOffset, restoredOperation.getLeftPageOffset());
    Assert.assertEquals(rightPageIndex, restoredOperation.getRightPageIndex());
    Assert.assertEquals(rightPageOffset, restoredOperation.getRightPageOffset());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int pageOffset = 12;

    final int entryIndex = 45;
    final byte[] key = new byte[12];

    final int leftPageIndex = 43;
    final int leftPageOffset = 430;
    final int rightPageIndex = 34;
    final int rightPageOffset = 340;

    OBonsaiBucketRemoveNonLeafEntryPageOperation operation = new OBonsaiBucketRemoveNonLeafEntryPageOperation(pageOffset,
        entryIndex, key, leftPageIndex, leftPageOffset, rightPageIndex, rightPageOffset);

    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OBonsaiBucketRemoveNonLeafEntryPageOperation restoredOperation = new OBonsaiBucketRemoveNonLeafEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(entryIndex, restoredOperation.getEntryIndex());
    Assert.assertArrayEquals(key, restoredOperation.getKey());
    Assert.assertEquals(leftPageIndex, restoredOperation.getLeftPageIndex());
    Assert.assertEquals(leftPageOffset, restoredOperation.getLeftPageOffset());
    Assert.assertEquals(rightPageIndex, restoredOperation.getRightPageIndex());
    Assert.assertEquals(rightPageOffset, restoredOperation.getRightPageOffset());
  }
}
