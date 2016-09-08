package io.burt.rocksdb;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;
import org.rocksdb.util.BytewiseComparator;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
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
@JRubyClass(name = "RocksDb::Iterator")
public class Iterator extends RubyObject {
  private final RocksDB db;
  private final ReadOptions readOptions;
  private final byte[] from;
  private final byte[] to;
  private final int limit;
  private final boolean reverse;

  private RocksIterator iterator;
  private ComparatorOptions comparatorOptions;
  private BytewiseComparator comparator;
  private Slice endSlice;
  private int remaining;

  public Iterator(Ruby runtime, RubyClass cls, RocksDB db, ReadOptions readOptions, byte[] from, byte[] to, int limit, boolean reverse) {
    super(runtime, cls);
    this.db = db;
    this.readOptions = readOptions;
    this.from = from;
    this.to = to;
    this.limit = limit;
    this.reverse = reverse;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass scannerClass = parentModule.defineClassUnder("Iterator", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    scannerClass.defineAnnotatedMethods(Iterator.class);
    scannerClass.includeModule(runtime.getEnumerable());
    return scannerClass;
  }

  static Iterator create(Ruby runtime, RocksDB db, ReadOptions readOptions, RubyHash scanOptions) {
    byte[] from = null;
    byte[] to = null;
    int limit = -1;
    boolean reverse = false;
    if (scanOptions != null && !scanOptions.isNil()) {
      IRubyObject scanFrom = scanOptions.fastARef(runtime.newSymbol("from"));
      if (scanFrom != null && !scanFrom.isNil()) {
        from = scanFrom.asString().getBytes();
      }
      IRubyObject scanTo = scanOptions.fastARef(runtime.newSymbol("to"));
      if (scanTo != null && !scanTo.isNil()) {
        to = scanTo.asString().getBytes();
      }
      IRubyObject scanLimit = scanOptions.fastARef(runtime.newSymbol("limit"));
      if (scanLimit != null && !scanLimit.isNil()) {
        limit = (int) scanLimit.convertToInteger().getLongValue();
      }
      IRubyObject scanReverse = scanOptions.fastARef(runtime.newSymbol("reverse"));
      reverse = scanReverse != null && scanReverse.isTrue();
    }
    return create(runtime, db, readOptions, from, to, limit, reverse);
  }
  
  static Iterator create(Ruby runtime, RocksDB db, ReadOptions readOptions, byte[] from, byte[] to, int limit, boolean reverse) {
    return new Iterator(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Iterator"), db, readOptions, from, to, limit, reverse);
  }

  private void internalRewind() {
    internalClose();
    iterator = db.newIterator(readOptions);
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
