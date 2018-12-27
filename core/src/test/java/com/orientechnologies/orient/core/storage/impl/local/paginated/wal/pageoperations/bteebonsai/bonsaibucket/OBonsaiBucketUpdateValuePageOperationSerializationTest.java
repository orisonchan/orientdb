package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OBonsaiBucketUpdateValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 67;
    final byte[] prevValue = new byte[12];
    final Random random = new Random();
    random.nextBytes(prevValue);

    OBonsaiBucketUpdateValuePageOperation operation = new OBonsaiBucketUpdateValuePageOperation(pageOffset, index, prevValue);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    OBonsaiBucketUpdateValuePageOperation restoredOperation = new OBonsaiBucketUpdateValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 456;
    final int pageIndex = 654;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 67;
    final byte[] prevValue = new byte[12];
    final Random random = new Random();
    random.nextBytes(prevValue);

    OBonsaiBucketUpdateValuePageOperation operation = new OBonsaiBucketUpdateValuePageOperation(pageOffset, index, prevValue);

    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OBonsaiBucketUpdateValuePageOperation restoredOperation = new OBonsaiBucketUpdateValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
  }
}
