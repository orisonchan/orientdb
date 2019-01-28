package com.orientechnologies.orient.core.storage.cache.chm;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TwoQPolicy {
  private volatile int size;

  private int maxSize;

  private final LRUList a1in = new LRUList();

  private final Set<PageKey> outSet = new HashSet<>();
  private final LRUList      a1out  = new LRUList();

  private final LRUList am = new LRUList();

  private int maxInSize;
  private int maxOutSize;

  private final ConcurrentHashMap<PageKey, OCacheEntry> data;

  TwoQPolicy(final int maxSize, final ConcurrentHashMap<PageKey, OCacheEntry> data) {
    this.maxSize = maxSize;

    calculateQueuesSize(maxSize);
    this.data = data;
  }

  void setMaxSize(final int maxSize) {
    if (am.size() + a1in.size() < maxSize) {
      this.maxSize = maxSize;

      calculateQueuesSize(maxSize);
    } else {
      throw new IllegalStateException("Disk cache size can not be changed to " + maxSize);
    }
  }

  private void calculateQueuesSize(final int maxSize) {
    maxOutSize = maxSize >> 1;
    maxInSize = maxSize >> 2;

    assert a1in.size() < maxInSize;
  }

  void onAdd(final OCacheEntry entry, final OWriteCache writeCache) {
    if (entry.isAlive()) {
      final PageKey pageKey = new PageKey(entry.getFileId(), (int) entry.getPageIndex());

      if (outSet.remove(pageKey)) {
        assert a1out.contains(entry);
        a1out.remove(entry);

        am.moveToTheTail(entry);

        assert outSet.size() == a1out.size();
      } else {
        a1in.moveToTheTail(entry);
      }

      while (am.size() + a1in.size() > maxSize) {
        if (a1in.size() > maxInSize) {
          final OCacheEntry cacheEntry = a1in.poll();
          assert cacheEntry != null;

          if (cacheEntry.freeze()) {
            final OCachePointer cachePointer = cacheEntry.getCachePointer();
            cachePointer.decrementReadersReferrer();
            cacheEntry.clearCachePointer();

            data.remove(pageKey, cacheEntry);
            cacheEntry.makeDead();

            outSet.add(pageKey);
            a1out.moveToTheTail(cacheEntry);

            assert outSet.size() == a1out.size();

            while (a1out.size() > maxOutSize) {
              final OCacheEntry removedEntry = a1out.poll();

              assert removedEntry != null;
              assert removedEntry.isDead();

              outSet.remove(new PageKey(removedEntry.getFileId(), (int) removedEntry.getPageIndex()));
            }
          } else {
            a1in.moveToTheTail(cacheEntry);
          }
        } else {
          final OCacheEntry cacheEntry = am.poll();

          assert cacheEntry != null;
          if (cacheEntry.freeze()) {
            final OCachePointer cachePointer = cacheEntry.getCachePointer();
            cachePointer.decrementReadersReferrer();
            cacheEntry.clearCachePointer();

            data.remove(pageKey, cacheEntry);
            cacheEntry.makeDead();
          }
        }
      }
    }

    //noinspection NonAtomicOperationOnVolatileField
    size++;

    try {
      writeCache.checkCacheOverflow();
    } catch (final InterruptedException e) {
      throw OException.wrapException(new OInterruptedException("Check of write cache overflow was interrupted"), e);
    }
  }

  void onAccess(final OCacheEntry cacheEntry) {
    if (!cacheEntry.isDead()) {
      if (am.contains(cacheEntry)) {
        am.moveToTheTail(cacheEntry);
      }
      //otherwise do nothing
    }
  }

  public long size() {
    return size;
  }

  void onRemove(final OCacheEntry entry) {
    assert entry.isFrozen();

    if (a1in.contains(entry)) {
      a1in.remove(entry);
    } else if (am.contains(entry)) {
      am.remove(entry);
    }

    entry.makeDead();

    //noinspection NonAtomicOperationOnVolatileField
    size--;

    final OCachePointer cachePointer = entry.getCachePointer();
    cachePointer.decrementReadersReferrer();
    entry.clearCachePointer();
  }
}
