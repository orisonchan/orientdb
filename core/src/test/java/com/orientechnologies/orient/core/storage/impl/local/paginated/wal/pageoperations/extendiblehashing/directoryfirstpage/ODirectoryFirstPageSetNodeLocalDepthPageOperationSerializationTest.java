package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.extendiblehashing.directoryfirstpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ODirectoryFirstPageSetNodeLocalDepthPageOperationSerializationTest {
  @Test
  public void testStreamSerialization() {
    final long fileId = 234;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int localNodeIndex = 67;
    final byte oldDepth = 2;

    ODirectoryFirstPageSetNodeLocalDepthPageOperation operation = new ODirectoryFirstPageSetNodeLocalDepthPageOperation(
        localNodeIndex, oldDepth);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final byte[] stream = new byte[serializedSize + 1];
    int offset = operation.toStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);

    ODirectoryFirstPageSetNodeLocalDepthPageOperation restoredOperation = new ODirectoryFirstPageSetNodeLocalDepthPageOperation();
    offset = restoredOperation.fromStream(stream, 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(localNodeIndex, restoredOperation.getLocalNodeIndex());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());
  }

  @Test
  public void testBufferSerialization() {
    final long fileId = 234;
    final int pageIndex = 45;
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();
    final int localNodeIndex = 67;
    final byte oldDepth = 2;

    ODirectoryFirstPageSetNodeLocalDepthPageOperation operation = new ODirectoryFirstPageSetNodeLocalDepthPageOperation(
        localNodeIndex, oldDepth);
    operation.setFileId(fileId);
    operation.setPageIndex(pageIndex);
    operation.setOperationUnitId(operationUnitId);

    final int serializedSize = operation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    ODirectoryFirstPageSetNodeLocalDepthPageOperation restoredOperation = new ODirectoryFirstPageSetNodeLocalDepthPageOperation();
    int offset = restoredOperation.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(fileId, restoredOperation.getFileId());
    Assert.assertEquals(pageIndex, restoredOperation.getPageIndex());
    Assert.assertEquals(operationUnitId, restoredOperation.getOperationUnitId());
    Assert.assertEquals(localNodeIndex, restoredOperation.getLocalNodeIndex());
    Assert.assertEquals(oldDepth, restoredOperation.getOldDepth());
  }
}
