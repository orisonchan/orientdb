package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.bteebonsai.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OBonsaiBucketAddAllPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 34;
    final List<byte[]> entries = new ArrayList<>(3);
    final Random random = new Random();

    for (int i = 0; i < 3; i++) {
      final byte[] entry = new byte[random.nextInt(20) + 10];
      random.nextBytes(entry);
      entries.add(entry);
    }

    OBonsaiBucketAddAllPageOperation operation = new OBonsaiBucketAddAllPageOperation(pageOffset, entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OBonsaiBucketAddAllPageOperation restoredOperation = new OBonsaiBucketAddAllPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());

    final List<byte[]> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());

    for (int i = 0; i < entries.size(); i++) {
      final byte[] entry = entries.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

  @Test
  public void testBufferSerialization() {
    final int fileId = 345;
    final int pageIndex = 543;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int pageOffset = 34;
    final List<byte[]> entries = new ArrayList<>(3);
    final Random random = new Random();

    for (int i = 0; i < 3; i++) {
      final byte[] entry = new byte[random.nextInt(20) + 10];
      random.nextBytes(entry);
      entries.add(entry);
    }

    OBonsaiBucketAddAllPageOperation operation = new OBonsaiBucketAddAllPageOperation(pageOffset, entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OBonsaiBucketAddAllPageOperation restoredOperation = new OBonsaiBucketAddAllPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(pageOffset, restoredOperation.getPageOffset());

    final List<byte[]> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());

    for (int i = 0; i < entries.size(); i++) {
      final byte[] entry = entries.get(i);
      final byte[] restoredEntry = restoredEntries.get(i);

      Assert.assertArrayEquals(entry, restoredEntry);
    }
  }

}
