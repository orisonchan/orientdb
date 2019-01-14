package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.hashindexbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OHashIndexBucket;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OHashIndexBucketClearAndInitPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 234;
    final int pageIndex = 456;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte oldDepth = 7;
    final List<OHashIndexBucket.RawEntry> entries = new ArrayList<>(3);
    final Random random = new Random();
    for (int i = 0; i < 3; i++) {
      final int size = random.nextInt(20) + 5;
      final byte[] entry = new byte[size];

      random.nextBytes(entry);
      final int keySize = random.nextInt(20) + 5;
      final int valueSize = random.nextInt(20) + 5;

      entries.add(new OHashIndexBucket.RawEntry(entry, 0, keySize, valueSize));
    }

    OHashIndexBucketClearAndInitPageOperation operation = new OHashIndexBucketClearAndInitPageOperation(oldDepth, entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    OHashIndexBucketClearAndInitPageOperation restoredOperation = new OHashIndexBucketClearAndInitPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());

    final List<OHashIndexBucket.RawEntry> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());
    for (int i = 0; i < entries.size(); i++) {
      final OHashIndexBucket.RawEntry rawEntry = entries.get(i);
      final OHashIndexBucket.RawEntry restoredEntry = restoredEntries.get(i);
      Assert.assertEquals(rawEntry, restoredEntry);
    }
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 234;
    final int pageIndex = 456;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte oldDepth = 7;
    final List<OHashIndexBucket.RawEntry> entries = new ArrayList<>(3);
    final Random random = new Random();
    for (int i = 0; i < 3; i++) {
      final int size = random.nextInt(20) + 5;
      final byte[] entry = new byte[size];

      random.nextBytes(entry);
      final int keySize = random.nextInt(20) + 5;
      final int valueSize = random.nextInt(20) + 5;

      entries.add(new OHashIndexBucket.RawEntry(entry, 0, keySize, valueSize));
    }

    OHashIndexBucketClearAndInitPageOperation operation = new OHashIndexBucketClearAndInitPageOperation(oldDepth, entries);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    OHashIndexBucketClearAndInitPageOperation restoredOperation = new OHashIndexBucketClearAndInitPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());

    final List<OHashIndexBucket.RawEntry> restoredEntries = restoredOperation.getEntries();
    Assert.assertEquals(entries.size(), restoredEntries.size());
    for (int i = 0; i < entries.size(); i++) {
      final OHashIndexBucket.RawEntry rawEntry = entries.get(i);
      final OHashIndexBucket.RawEntry restoredEntry = restoredEntries.get(i);
      Assert.assertEquals(rawEntry, restoredEntry);
    }
  }
}
