package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directorypage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ODirectoryPageSetMaxLeftChildDepthPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final int pageIndex = 34;
    final long fileId = 456;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int localNodeIndex = 56;
    final byte depth = 7;

    ODirectoryPageSetMaxLeftChildDepthPageOperation operation = new ODirectoryPageSetMaxLeftChildDepthPageOperation(localNodeIndex,
        depth);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    ODirectoryPageSetMaxLeftChildDepthPageOperation restoredOperation = new ODirectoryPageSetMaxLeftChildDepthPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(localNodeIndex, restoredOperation.getLocalNodeIndex());
    Assert.assertEquals(depth, restoredOperation.getOldDepth());
  }

  @Test
  public void testBufferSerialization() {
    final int pageIndex = 34;
    final long fileId = 456;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int localNodeIndex = 56;
    final byte depth = 7;

    ODirectoryPageSetMaxLeftChildDepthPageOperation operation = new ODirectoryPageSetMaxLeftChildDepthPageOperation(localNodeIndex,
        depth);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());
    ODirectoryPageSetMaxLeftChildDepthPageOperation restoredOperation = new ODirectoryPageSetMaxLeftChildDepthPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(localNodeIndex, restoredOperation.getLocalNodeIndex());
    Assert.assertEquals(depth, restoredOperation.getOldDepth());
  }
}
