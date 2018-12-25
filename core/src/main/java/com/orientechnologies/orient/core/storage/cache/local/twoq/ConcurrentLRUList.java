/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Concurrent implementation of {@link LRUList}.
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public final class ConcurrentLRUList implements LRUList {

  @SuppressWarnings("CanBeFinal")
  private static boolean assertionsEnabled;

  static {
    //noinspection AssertWithSideEffects
    assert assertionsEnabled = true;
  }

  private final ConcurrentHashMap<CacheKey, LRUEntry> cache         = new ConcurrentHashMap<>();
  private final ListNode                              headReference = new ListNode(null, true);
  private final AtomicReference<ListNode>             tailReference = new AtomicReference<>(headReference);

  private final ConcurrentLinkedQueue<ListNode> trash = new ConcurrentLinkedQueue<>();

  private final int           minTrashSize    = Runtime.getRuntime().availableProcessors() * 4;
  private final AtomicBoolean purgeInProgress = new AtomicBoolean();
  private final AtomicInteger trashSize       = new AtomicInteger();

  public ConcurrentLRUList() {
  }

  @Override
  public OCacheEntry get(final long fileId, final long pageIndex) {
    final LRUEntry lruEntry = cache.get(new CacheKey(fileId, pageIndex));

    purge();

    if (lruEntry == null)
      return null;

    return lruEntry.entry;
  }

  @Override
  public OCacheEntry remove(final long fileId, final long pageIndex) {
    final CacheKey key = new CacheKey(fileId, pageIndex);
    final LRUEntry valueToRemove = cache.remove(key);

    if (valueToRemove == null)
      return null;

    valueToRemove.removeLock.writeLock().lock();
    try {
      valueToRemove.removed = true;
      final ListNode node = valueToRemove.listNode.get();
      valueToRemove.listNode.lazySet(null);

      if (node != null)
        addToTrash(node);
    } finally {
      valueToRemove.removeLock.writeLock().unlock();
    }

    purge();

    return valueToRemove.entry;
  }

  @Override
  public void putToMRU(final OCacheEntry cacheEntry) {
    final CacheKey key = new CacheKey(cacheEntry.getFileId(), cacheEntry.getPageIndex());
    final LRUEntry value = new LRUEntry(key, cacheEntry);
    final LRUEntry existingValue = cache.putIfAbsent(key, value);

    if (existingValue != null) {
      existingValue.entry = cacheEntry;
      offer(existingValue);
    } else
      offer(value);

    purge();
  }

  private void offer(final LRUEntry lruEntry) {
    lruEntry.removeLock.readLock().lock();
    try {
      if (lruEntry.removed)
        return;

      ListNode tail = tailReference.get();

      if (!lruEntry.equals(tail.entry)) {
        final ListNode oldNode = lruEntry.listNode.get();

        final ListNode newNode = new ListNode(lruEntry, false);
        if (lruEntry.listNode.compareAndSet(oldNode, newNode)) {

          while (true) {
            newNode.previous.set(tail);

            if (tail.next.compareAndSet(null, newNode)) {
              tailReference.compareAndSet(tail, newNode);
              break;
            }

            tail = tailReference.get();
          }

          if (oldNode != null)
            addToTrash(oldNode);
        }
      }

    } finally {
      lruEntry.removeLock.readLock().unlock();
    }
  }

  @Override
  public OCacheEntry removeLRU() {
    ListNode current = headReference;

    boolean removed = false;

    LRUEntry currentEntry = null;
    int inUseCounter = 0;
    do {
      while (current.isDummy || (currentEntry = current.entry) == null || (isInUse(currentEntry.entry))) {
        if (currentEntry != null && isInUse(currentEntry.entry))
          inUseCounter++;

        final ListNode next = current.next.get();

        if (next == null) {
          if (cache.size() == inUseCounter)
            return null;

          current = headReference;
          inUseCounter = 0;
          continue;
        }

        current = next;
      }

      if (cache.remove(currentEntry.key, currentEntry)) {
        currentEntry.removeLock.writeLock().lock();
        try {
          currentEntry.removed = true;
          final ListNode node = currentEntry.listNode.get();

          currentEntry.listNode.lazySet(null);
          addToTrash(node);
          removed = true;
        } finally {
          currentEntry.removeLock.writeLock().unlock();
        }
      } else {
        current = headReference;
        inUseCounter = 0;
      }

    } while (!removed);

    purge();

    return currentEntry.entry;
  }

  @Override
  public OCacheEntry getLRU() {
    ListNode current = headReference;

    LRUEntry currentEntry = null;
    int inUseCounter = 0;

    while (current.isDummy || (currentEntry = current.entry) == null || (isInUse(currentEntry.entry))) {
      if (currentEntry != null && isInUse(currentEntry.entry))
        inUseCounter++;

      final ListNode next = current.next.get();

      if (next == null) {
        if (cache.size() == inUseCounter)
          return null;

        current = headReference;
        inUseCounter = 0;
        continue;
      }

      current = next;
    }

    purge();

    return currentEntry.entry;
  }

  private void purge() {
    if (purgeInProgress.compareAndSet(false, true)) {
      purgeSomeFromTrash();

      purgeInProgress.set(false);
    }
  }

  private void purgeSomeFromTrash() {
    int additionalSize = 0;

    while (trashSize.get() >= minTrashSize + additionalSize) {

      final ListNode node = trash.poll();
      trashSize.decrementAndGet();

      if (node == null)
        return;

      if (node.next.get() == null) {
        trash.add(node);
        trashSize.incrementAndGet();
        additionalSize++;
        continue;
      }

      final ListNode previous = node.previous.get();
      final ListNode next = node.next.get();

      node.previous.lazySet(null);

      assert previous.next.get() == node;
      assert next == null || next.previous.get() == node;

      if (assertionsEnabled) {
        final boolean success = previous.next.compareAndSet(node, next);
        assert success;
      } else
        previous.next.set(next);

      if (next != null)
        next.previous.set(previous);
    }
  }

  @Override
  public void clear() {
    cache.clear();

    headReference.next.set(null);
    tailReference.set(headReference);

    trash.clear();
    trashSize.set(0);
  }

  @Override
  public boolean contains(final long fileId, final long filePosition) {
    return cache.containsKey(new CacheKey(fileId, filePosition));
  }

  private void addToTrash(final ListNode node) {
    node.entry = null;

    trash.add(node);
    trashSize.incrementAndGet();
  }

  @Override
  public int size() {
    return cache.size();
  }

  private static boolean isInUse(final OCacheEntry entry) {
    return entry != null && entry.getUsagesCount() != 0;
  }

  @Override
  @Nonnull
  public final Iterator<OCacheEntry> iterator() {
    return new OCacheEntryIterator(tailReference.get());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<OCacheEntry> reverseIterator() {
    return new OReverseCacheEntryIterator(headReference);
  }

  private static final class OCacheEntryIterator implements Iterator<OCacheEntry> {

    private ListNode current;

    OCacheEntryIterator(final ListNode start) {
      current = start;
      while (current != null && current.entry == null)
        current = current.previous.get();
    }

    @Override
    public final boolean hasNext() {
      return current != null && current.entry != null;
    }

    @Override
    public final OCacheEntry next() {
      if (!hasNext())
        throw new NoSuchElementException();

      final OCacheEntry entry = current.entry.entry;

      do {
        current = current.previous.get();
      } while (current != null && current.entry == null);

      return entry;
    }

    @Override
    public final void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Iterates from head to tail of LRU queue.
   */
  private static final class OReverseCacheEntryIterator implements Iterator<OCacheEntry> {
    private ListNode current;

    OReverseCacheEntryIterator(final ListNode start) {
      current = start;
      //because we purge nodes into background and because
      //head node is dummy we skip entries with empty entries
      while (current != null && current.entry == null)
        current = current.next.get();
    }

    @Override
    public final boolean hasNext() {
      return current != null && current.entry != null;
    }

    @Override
    public final OCacheEntry next() {
      if (!hasNext())
        throw new NoSuchElementException();

      final OCacheEntry entry = current.entry.entry;
      //because we purge nodes into background and because
      //head node is dummy we skip entries with empty entries

      do {
        current = current.next.get();
      } while (current != null && current.entry == null);

      return entry;
    }

    @Override
    public final void remove() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class CacheKey {
    private final long fileId;
    private final long pageIndex;

    private CacheKey(final long fileId, final long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public final boolean equals(final Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      final CacheKey that = (CacheKey) o;

      if (fileId != that.fileId)
        return false;
      return pageIndex == that.pageIndex;
    }

    @Override
    public final int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }
  }

  private static final class LRUEntry {
    private final    AtomicReference<ListNode> listNode = new AtomicReference<>();
    private final    CacheKey                  key;
    private volatile OCacheEntry               entry;

    private       boolean       removed;
    private final ReadWriteLock removeLock = new ReentrantReadWriteLock();

    private LRUEntry(final CacheKey key, final OCacheEntry entry) {
      this.key = key;
      this.entry = entry;
    }
  }

  private static final class ListNode {
    private volatile LRUEntry entry;
    private final AtomicReference<ListNode> next     = new AtomicReference<>();
    private final AtomicReference<ListNode> previous = new AtomicReference<>();

    private final boolean isDummy;

    private ListNode(final LRUEntry key, final boolean isDummy) {
      this.entry = key;
      this.isDummy = isDummy;
    }
  }
}
