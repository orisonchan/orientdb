package com.orientechnologies.orient.core.storage.cache.chm;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;

public final class LRUList {
  private int size;

  private OCacheEntry head;
  private OCacheEntry tail;

  void remove(final OCacheEntry entry) {
    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    if (!(next != null || prev != null || entry == head)) {
      return;
    }

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (next != null) {
      next.setPrev(prev);
    }

    if (prev != null) {
      prev.setNext(next);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    if (tail == entry) {
      assert entry.getNext() == null;
      tail = prev;
    }

    entry.setNext(null);
    entry.setPrev(null);
    entry.setContainer(null);

    size--;
  }

  boolean contains(final OCacheEntry entry) {
    return entry.getContainer() == this;
  }

  void moveToTheTail(final OCacheEntry entry) {
    if (tail == entry) {
      assert entry.getNext() == null;
      return;
    }

    final OCacheEntry next = entry.getNext();
    final OCacheEntry prev = entry.getPrev();

    final boolean newEntry = !(next != null || prev != null || entry == head);

    assert prev == null || prev.getNext() == entry;
    assert next == null || next.getPrev() == entry;

    if (prev != null) {
      prev.setNext(next);
    }

    if (next != null) {
      next.setPrev(prev);
    }

    if (head == entry) {
      assert entry.getPrev() == null;
      head = next;
    }

    entry.setPrev(tail);
    entry.setNext(null);

    if (tail != null) {
      assert tail.getNext() == null;
      tail.setNext(entry);
      tail = entry;
    } else {
      tail = head = entry;
    }

    if (newEntry) {
      entry.setContainer(this);
      size++;
    } else {
      assert entry.getContainer() == this;
    }
  }

  int size() {
    return size;
  }

  OCacheEntry poll() {
    if (head == null) {
      return null;
    }

    final OCacheEntry entry = head;

    final OCacheEntry next = head.getNext();
    assert next == null || next.getPrev() == head;

    head = next;
    if (next != null) {
      next.setPrev(null);
    }

    assert head == null || head.getPrev() == null;

    if (head == null) {
      tail = null;
    }

    entry.setNext(null);
    assert entry.getPrev() == null;

    size--;

    return entry;
  }
}
