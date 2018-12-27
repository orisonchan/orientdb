package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OBonsaiBucketShrinkPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 234;
    final int pageIndex = 432;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final List<byte[]> removedEntries = new ArrayList<>(3);

    final Random random = new Random();
    for (int i = 0; i < 3; i++) {
      final byte[] entry = new byte[random.nextInt(20) + 10];
      random.nextBytes(entry);

      removedEntries.add(entry);
    }

    OBonsaiBucketShrinkPageOperation operation = new OBonsaiBucketShrinkPageOperation(pageOffset, removedEntries);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OBonsaiBucketShrinkPageOperation restoredOperation = new OBonsaiBucketShrinkPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    final List<byte[]> restoredRemovedEntries = restoredOperation.getRemovedEntries();
    Assert.assertEquals(removedEntries.size(), restoredRemovedEntries.size());
    for (int i = 0; i < 3; i++) {
      final byte[] entry = removedEntries.get(i);
      final byte[] restoredEntry = restoredRemovedEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 234;
    final int pageIndex = 432;
    final int pageOffset = 123;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final List<byte[]> removedEntries = new ArrayList<>(3);

    final Random random = new Random();
    for (int i = 0; i < 3; i++) {
      final byte[] entry = new byte[random.nextInt(20) + 10];
      random.nextBytes(entry);

      removedEntries.add(entry);
    }

    OBonsaiBucketShrinkPageOperation operation = new OBonsaiBucketShrinkPageOperation(pageOffset, removedEntries);
    operation.setOperationUnitId(operationUnitId);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OBonsaiBucketShrinkPageOperation restoredOperation = new OBonsaiBucketShrinkPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());

    final List<byte[]> restoredRemovedEntries = restoredOperation.getRemovedEntries();
    Assert.assertEquals(removedEntries.size(), restoredRemovedEntries.size());
    for (int i = 0; i < 3; i++) {
      final byte[] entry = removedEntries.get(i);
      final byte[] restoredEntry = restoredRemovedEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }
}
