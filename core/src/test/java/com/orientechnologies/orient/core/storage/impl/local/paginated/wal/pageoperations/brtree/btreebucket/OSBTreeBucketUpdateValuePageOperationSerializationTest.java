package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.brtree.btreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.btree.btreebucket.OSBTreeBucketUpdateValuePageOperation;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBucketUpdateValuePageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 34;

    final Random random = new Random();

    final byte[] value = new byte[12];
    random.nextBytes(value);

    final byte[] prevValue = new byte[12];
    random.nextBytes(prevValue);

    final boolean isEncrypted = true;

    OSBTreeBucketUpdateValuePageOperation operation = new OSBTreeBucketUpdateValuePageOperation(index, value,
        prevValue, isEncrypted);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];

    int offset = operation.toStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    OSBTreeBucketUpdateValuePageOperation restoredOperation = new OSBTreeBucketUpdateValuePageOperation();
    offset = restoredOperation.fromStream(stream, 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(value, restoredOperation.getValue());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 123;
    final int pageIndex = 321;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int index = 34;

    final Random random = new Random();

    final byte[] value = new byte[12];
    random.nextBytes(value);

    final byte[] prevValue = new byte[12];
    random.nextBytes(prevValue);

    final boolean isEncrypted = true;

    OSBTreeBucketUpdateValuePageOperation operation = new OSBTreeBucketUpdateValuePageOperation(index, value,
        prevValue, isEncrypted);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketUpdateValuePageOperation restoredOperation = new OSBTreeBucketUpdateValuePageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(index, restoredOperation.getIndex());
    Assert.assertArrayEquals(value, restoredOperation.getValue());
    Assert.assertArrayEquals(prevValue, restoredOperation.getPrevValue());
    Assert.assertEquals(isEncrypted, restoredOperation.isEncrypted());
  }
}
