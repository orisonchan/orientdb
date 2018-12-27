package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OBonsaiBucketSetDeletedPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 54;
    final byte keySerializerId = 34;
    final byte valueSerializerId = 43;
    final boolean isLeaf = true;

    OBonsaiBucketSetDeletedPageOperation operation = new OBonsaiBucketSetDeletedPageOperation(pageOffset, keySerializerId,
        valueSerializerId, isLeaf);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OBonsaiBucketSetDeletedPageOperation restoredOperation = new OBonsaiBucketSetDeletedPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());
    Assert.assertEquals(isLeaf, restoredOperation.isLeaf());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 54;
    final byte keySerializerId = 34;
    final byte valueSerializerId = 43;
    final boolean isLeaf = true;

    OBonsaiBucketSetDeletedPageOperation operation = new OBonsaiBucketSetDeletedPageOperation(pageOffset, keySerializerId,
        valueSerializerId, isLeaf);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OBonsaiBucketSetDeletedPageOperation restoredOperation = new OBonsaiBucketSetDeletedPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(keySerializerId, restoredOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredOperation.getValueSerializerId());
    Assert.assertEquals(isLeaf, restoredOperation.isLeaf());
  }
}
