package com.orientechnologies.orient.core.storage.cache.chm;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.chm.readbuffer.BoundedBuffer;
import com.orientechnologies.orient.core.storage.cache.chm.readbuffer.Buffer;
import com.orientechnologies.orient.core.storage.cache.chm.writequeue.MPSCLinkedQueue;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TwoQReadCache implements OReadCache {
  private static final int NCPU                   = Runtime.getRuntime().availableProcessors();
  private static final int WRITE_BUFFER_MAX_BATCH = 128 * ceilingPowerOfTwo(NCPU);

  private final ConcurrentHashMap<PageKey, OCacheEntry> data;
  private final Lock                                    evictionLock = new ReentrantLock();

  private final TwoQPolicy twoQPolicy;

  private final Buffer<OCacheEntry>       readBuffer  = new BoundedBuffer<>();
  private final MPSCLinkedQueue<Runnable> writeBuffer = new MPSCLinkedQueue<>();

  /**
   * Status which indicates whether flush of buffers should be performed or may be delayed.
   */
  private final AtomicReference<DrainStatus> drainStatus = new AtomicReference<>(DrainStatus.IDLE);

  private final int pageSize;

  public TwoQReadCache(final long maxCacheSize, final int pageSize) {
    evictionLock.lock();
    try {
      this.pageSize = pageSize;

      this.data = new ConcurrentHashMap<>((int) (maxCacheSize / pageSize));
      twoQPolicy = new TwoQPolicy((int) (maxCacheSize / pageSize), data);
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public long addFile(final String fileName, final OWriteCache writeCache) throws IOException {
    return writeCache.addFile(fileName);
  }

  @Override
  public long addFile(final String fileName, final long fileId, final OWriteCache writeCache) throws IOException {
    return writeCache.addFile(fileName, fileId);
  }

  @Override
  public OCacheEntry loadForWrite(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums, final OLogSequenceNumber startLSN) {
    final OCacheEntry cacheEntry = doLoad(fileId, (int) pageIndex, writeCache, verifyChecksums);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer(), startLSN);
    }

    return cacheEntry;
  }

  @Override
  public OCacheEntry loadForRead(final long fileId, final long pageIndex, final boolean checkPinnedPages,
      final OWriteCache writeCache, final int pageCount, final boolean verifyChecksums) {
    return doLoad(fileId, (int) pageIndex, writeCache, verifyChecksums);
  }

  private OCacheEntry doLoad(final long fileId, final int pageIndex, final OWriteCache writeCache, final boolean verifyChecksums) {
    final PageKey pageKey = new PageKey(fileId, pageIndex);
    while (true) {
      checkWriteBuffer();

      OCacheEntry cacheEntry;

      cacheEntry = data.get(pageKey);

      if (cacheEntry != null) {
        if (cacheEntry.acquireEntry()) {
          afterRead(cacheEntry);
          return cacheEntry;
        }
      } else {
        final boolean[] read = new boolean[1];

        cacheEntry = data.compute(pageKey, (page, entry) -> {
          if (entry == null) {
            try {
              final OCachePointer[] pointers = writeCache.load(fileId, pageIndex, 1, new OModifiableBoolean(), verifyChecksums);
              if (pointers.length == 0) {
                return null;
              }

              return new OCacheEntryImpl(page.getFileId(), page.getPageIndex(), pointers[0]);
            } catch (final IOException e) {
              throw OException
                  .wrapException(new OStorageException("Error during loading of page " + pageIndex + " for file " + fileId), e);
            }
          } else {
            read[0] = true;
            return entry;
          }
        });

        if (cacheEntry == null) {
          return null;
        }

        if (cacheEntry.acquireEntry()) {
          if (read[0]) {
            afterRead(cacheEntry);
          } else {
            afterAdd(cacheEntry, writeCache);
          }

          return cacheEntry;
        }
      }
    }
  }

  @Override
  public void changeMaximumAmountOfMemory(final long maxMemory) {
    evictionLock.lock();
    try {
      twoQPolicy.setMaxSize((int) (maxMemory / pageSize));
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void releaseFromRead(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    cacheEntry.releaseEntry();
  }

  @Override
  public void releaseFromWrite(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    final OCachePointer cachePointer = cacheEntry.getCachePointer();
    assert cachePointer != null;

    final PageKey pageKey = new PageKey(cacheEntry.getFileId(), (int) cacheEntry.getPageIndex());
    if (cacheEntry.isDirty()) {
      data.compute(pageKey, (page, entry) -> {
        writeCache.store(cacheEntry.getFileId(), cacheEntry.getPageIndex(), cacheEntry.getCachePointer());
        return entry;//may be absent if page in pinned pages, in such case we use map as virtual lock
      });

      cacheEntry.clearDirty();
    }

    //We need to release exclusive lock from cache pointer after we put it into the write cache so both "dirty pages" of write
    //cache and write cache itself will contain actual values simultaneously. But because cache entry can be cleared after we put it back to the
    //read cache we make copy of cache pointer before head.
    //
    //Following situation can happen, if we release exclusive lock before we put entry to the write cache.
    //1. Page is loaded for write, locked and related LSN is written to the "dirty pages" table.
    //2. Page lock is released.
    //3. Page is chosen to be flushed on disk and its entry removed from "dirty pages" table
    //4. Page is added to write cache as dirty
    //
    //So we have situation when page is added as dirty into the write cache but its related entry in "dirty pages" table is removed
    //it is treated as flushed during fuzzy checkpoint and portion of write ahead log which contains not flushed changes is removed.
    //This can lead to the data loss after restore and corruption of data structures
    cachePointer.releaseExclusiveLock();
    cacheEntry.releaseEntry();
  }

  @Override
  public void pinPage(final OCacheEntry cacheEntry, final OWriteCache writeCache) {
    //do nothing
  }

  @Override
  public OCacheEntry allocateNewPage(final long fileId, final OWriteCache writeCache, final boolean verifyChecksums,
      final OLogSequenceNumber startLSN) throws IOException {
    final int newPageIndex = writeCache.allocateNewPage(fileId);
    final OCacheEntry cacheEntry = doLoad(fileId, newPageIndex, writeCache, true);

    if (cacheEntry != null) {
      cacheEntry.acquireExclusiveLock();
      writeCache.updateDirtyPagesTable(cacheEntry.getCachePointer(), startLSN);
    }

    return cacheEntry;
  }

  private void afterRead(final OCacheEntry entry) {
    final boolean bufferOverflow = readBuffer.offer(entry) == Buffer.FULL;

    if (drainStatus.get().shouldBeDrained(bufferOverflow)) {
      tryToDrainBuffers();
    }
  }

  private void afterAdd(final OCacheEntry entry, final OWriteCache writeCache) {
    afterWrite(() -> twoQPolicy.onAdd(entry, writeCache));
  }

  private void afterWrite(final Runnable command) {
    writeBuffer.offer(command);

    drainStatus.lazySet(DrainStatus.REQUIRED);
    tryToDrainBuffers();
  }

  private void checkWriteBuffer() {
    if (!writeBuffer.isEmpty()) {

      drainStatus.lazySet(DrainStatus.REQUIRED);
      tryToDrainBuffers();
    }
  }

  private void tryToDrainBuffers() {
    if (drainStatus.get() == DrainStatus.IN_PROGRESS) {
      return;
    }

    if (evictionLock.tryLock()) {
      try {
        //optimization to avoid to call tryLock if it is not needed
        drainStatus.lazySet(DrainStatus.IN_PROGRESS);
        drainBuffers();
      } finally {
        //cas operation because we do not want to overwrite REQUIRED status and to avoid false optimization of
        //drain buffer by IN_PROGRESS status
        drainStatus.compareAndSet(DrainStatus.IN_PROGRESS, DrainStatus.IDLE);
        evictionLock.unlock();
      }
    }
  }

  private void drainBuffers() {
    drainWriteBuffer();
    drainReadBuffers();
  }

  private void emptyBuffers() {
    emptyWriteBuffer();
    drainReadBuffers();
  }

  private void drainReadBuffers() {
    readBuffer.drainTo(twoQPolicy::onAccess);
  }

  private void drainWriteBuffer() {
    for (int i = 0; i < WRITE_BUFFER_MAX_BATCH; i++) {
      final Runnable command = writeBuffer.poll();

      if (command == null) {
        break;
      }

      command.run();
    }
  }

  private void emptyWriteBuffer() {
    while (true) {
      final Runnable command = writeBuffer.poll();

      if (command == null) {
        break;
      }

      command.run();
    }
  }

  @Override
  public long getUsedMemory() {
    return twoQPolicy.size() * 4 * 1024;
  }

  @Override
  public void clear() {
    evictionLock.lock();
    try {
      emptyBuffers();

      for (final OCacheEntry entry : data.values()) {
        if (entry.freeze()) {
          twoQPolicy.onRemove(entry);
        } else {
          throw new OStorageException(
              "Page with index " + entry.getPageIndex() + " for file id " + entry.getFileId() + " is used and cannot be removed");
        }
      }
    } finally {
      evictionLock.unlock();
    }
  }

  @Override
  public void truncateFile(final long fileId, final OWriteCache writeCache) throws IOException {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);
    writeCache.truncateFile(fileId);

    clearFile(fileId, filledUpTo, writeCache);
  }

  @Override
  public void closeFile(final long fileId, final boolean flush, final OWriteCache writeCache) {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);

    clearFile(fileId, filledUpTo, writeCache);
    writeCache.close(fileId, flush);
  }

  public void deleteFile(final long fileId, final OWriteCache writeCache) throws IOException {
    final int filledUpTo = (int) writeCache.getFilledUpTo(fileId);

    clearFile(fileId, filledUpTo, writeCache);
    writeCache.deleteFile(fileId);
  }

  @Override
  public void deleteStorage(final OWriteCache writeCache) throws IOException {
    final Collection<Long> files = writeCache.files().values();
    final List<ORawPair<Long, Integer>> filledUpTo = new ArrayList<>(1024);
    for (final long fileId : files) {
      filledUpTo.add(new ORawPair<>(fileId, (int) writeCache.getFilledUpTo(fileId)));
    }

    for (final ORawPair<Long, Integer> entry : filledUpTo) {
      clearFile(entry.getFirst(), entry.getSecond(), writeCache);
    }

    writeCache.delete();
  }

  @Override
  public void closeStorage(final OWriteCache writeCache) throws IOException {
    final Collection<Long> files = writeCache.files().values();
    final List<ORawPair<Long, Integer>> filledUpTo = new ArrayList<>(1024);
    for (final long fileId : files) {
      filledUpTo.add(new ORawPair<>(fileId, (int) writeCache.getFilledUpTo(fileId)));
    }

    for (final ORawPair<Long, Integer> entry : filledUpTo) {
      clearFile(entry.getFirst(), entry.getSecond(), writeCache);
    }

    writeCache.close();
  }

  @Override
  public void loadCacheState(final OWriteCache writeCache) {
    //do nothing
  }

  @Override
  public void storeCacheState(final OWriteCache writeCache) {
    //do nothing
  }

  private void clearFile(final long fileId, final int filledUpTo, final OWriteCache writeCache) {
    evictionLock.lock();
    try {
      emptyBuffers();

      for (int pageIndex = 0; pageIndex < filledUpTo; pageIndex++) {
        final PageKey pageKey = new PageKey(fileId, pageIndex);
        final OCacheEntry cacheEntry = data.remove(pageKey);
        if (cacheEntry != null) {
          if (cacheEntry.freeze()) {
            twoQPolicy.onRemove(cacheEntry);

            try {
              writeCache.checkCacheOverflow();
            } catch (final InterruptedException e) {
              throw OException.wrapException(new OInterruptedException("Check of write cache overflow was interrupted"), e);
            }
          } else {
            throw new OStorageException("Page with index " + cacheEntry.getPageIndex() + " for file id " + cacheEntry.getFileId()
                + " is used and cannot be removed");
          }
        }
      }
    } finally {
      evictionLock.unlock();
    }
  }

  private enum DrainStatus {
    IDLE {
      @Override
      boolean shouldBeDrained(final boolean readBufferOverflow) {
        return readBufferOverflow;
      }
    }, IN_PROGRESS {
      @Override
      boolean shouldBeDrained(final boolean readBufferOverflow) {
        return false;
      }
    }, REQUIRED {
      @Override
      boolean shouldBeDrained(final boolean readBufferOverflow) {
        return true;
      }
    };

    abstract boolean shouldBeDrained(boolean readBufferOverflow);
  }

  @SuppressWarnings("SameParameterValue")
  private static int ceilingPowerOfTwo(final int x) {
    // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
    return 1 << -Integer.numberOfLeadingZeros(x - 1);
  }
}
