package io.burt.rocksdb;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.util.BytewiseComparator;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

@SuppressWarnings("serial")
@JRubyClass(name = "RocksDb::Cursor")
public class Cursor extends RubyObject {
  private final RocksDB db;
  private final byte[] from;
  private final byte[] to;
  private final int limit;
  private final boolean reverse;

  private RocksIterator iterator;
  private ComparatorOptions comparatorOptions;
  private BytewiseComparator comparator;
  private Slice endSlice;
  private int remaining;

  public Cursor(Ruby runtime, RubyClass cls, RocksDB db, byte[] from, byte[] to, int limit, boolean reverse) {
    super(runtime, cls);
    this.db = db;
    this.from = from;
    this.to = to;
    this.limit = limit;
    this.reverse = reverse;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass scannerClass = parentModule.defineClassUnder("Cursor", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    scannerClass.defineAnnotatedMethods(Cursor.class);
    scannerClass.includeModule(runtime.getEnumerable());
    return scannerClass;
  }

  static Cursor create(Ruby runtime, RocksDB db, byte[] from, byte[] to, int limit, boolean reverse) {
    return new Cursor(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Cursor"), db, from, to, limit, reverse);
  }

  private void internalRewind() {
    internalClose();
    iterator = db.newIterator();
    comparatorOptions = new ComparatorOptions();
    comparator = new BytewiseComparator(comparatorOptions);
    remaining = limit < 0 ? Integer.MAX_VALUE : limit;
    endSlice = to == null ? null : new Slice(to);
    if (reverse) {
      if (from == null) {
        iterator.seekToLast();
      } else {
        iterator.seek(from);
        if (iterator.isValid()) {
          try (Slice keySlice = new Slice(iterator.key()); Slice fromSlice = new Slice(from)) {
            if (comparator.compare(fromSlice, keySlice) < 0) {
              iterator.prev();
            }
          }
        } else {
          iterator.seekToLast();
        }
      }
    } else {
      if (from == null) {
        iterator.seekToFirst();
      } else {
        iterator.seek(from);
      }
    }
  }

  private void internalAdvance() {
    if (reverse) {
      iterator.prev();
    } else {
      iterator.next();
    }
    remaining--;
  }

  private void internalClose() {
    if (endSlice != null) {
      endSlice.close();
    }
    if (comparator != null) {
      comparator.close();
    }
    if (comparatorOptions != null) {
      comparatorOptions.close();
    }
    if (iterator != null) {
      iterator.close();
    }
  }

  private boolean internalHasNext() {
    if (iterator != null && iterator.isValid() && remaining > 0) {
      if (to != null) {
        try (Slice keySlice = new Slice(iterator.key())) {
          int comparisonResult = comparator.compare(endSlice, keySlice);
          if ((reverse && comparisonResult > 0) || (!reverse && comparisonResult < 0)) {
            return false;
          }
        }
      }
      return true;
    } else {
      return false;
    }
  }

  @JRubyMethod
  public IRubyObject each(ThreadContext ctx, Block block) {
    internalRewind();
    while (internalHasNext()) {
      IRubyObject key = ctx.runtime.newString(new ByteList(iterator.key()));
      IRubyObject value = ctx.runtime.newString(new ByteList(iterator.value()));
      block.yieldSpecific(ctx, key, value);
      internalAdvance();
    }
    internalClose();
    return this;
  }

  @JRubyMethod
  public IRubyObject rewind(ThreadContext ctx) {
    internalRewind();
    return ctx.runtime.getNil();
  }

  @JRubyMethod(name = "next?")
  public IRubyObject next_p(ThreadContext ctx) {
    return ctx.runtime.newBoolean(internalHasNext());
  }

  @JRubyMethod
  public IRubyObject next(ThreadContext ctx) {
    if (iterator == null) {
      internalRewind();
    }
    if (internalHasNext()) {
      IRubyObject key = ctx.runtime.newString(new ByteList(iterator.key()));
      IRubyObject value = ctx.runtime.newString(new ByteList(iterator.value()));
      internalAdvance();
      return ctx.runtime.newArray(key, value);
    } else {
      throw ctx.runtime.newLightweightStopIterationError("Iterator exhausted");
    }
  }
}
