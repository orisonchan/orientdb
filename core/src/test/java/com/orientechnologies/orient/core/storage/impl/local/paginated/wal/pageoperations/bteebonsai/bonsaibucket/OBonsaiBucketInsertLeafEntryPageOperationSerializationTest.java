package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketInsertLeafEntryPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 32;
    final int index = 32;
    final int keySize = 456;
    final int valueSize = 654;

    OBonsaiBucketInsertLeafEntryPageOperation operation = new OBonsaiBucketInsertLeafEntryPageOperation(pageOffset, index, keySize,
        valueSize);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OBonsaiBucketInsertLeafEntryPageOperation restoredOperation = new OBonsaiBucketInsertLeafEntryPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 32;
    final int index = 32;
    final int keySize = 456;
    final int valueSize = 654;

    OBonsaiBucketInsertLeafEntryPageOperation operation = new OBonsaiBucketInsertLeafEntryPageOperation(pageOffset, index, keySize,
        valueSize);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OBonsaiBucketInsertLeafEntryPageOperation restoredOperation = new OBonsaiBucketInsertLeafEntryPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(keySize, restoredOperation.getKeySize());
    Assert.assertEquals(valueSize, restoredOperation.getValueSize());
    Assert.assertEquals(index, restoredOperation.getIndex());
  }
}
