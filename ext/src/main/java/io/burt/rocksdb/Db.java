package io.burt.rocksdb;

import org.rocksdb.FlushOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

@SuppressWarnings("serial")
@JRubyClass(name = "RocksDb::Db")
public class Db extends RubyObject implements AutoCloseable {
  private final RocksDB db;

  public Db(Ruby runtime, RubyClass cls, RocksDB db) {
    super(runtime, cls);
    this.db = db;
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass dbClass = parentModule.defineClassUnder("Db", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    dbClass.defineAnnotatedMethods(Db.class);
    return dbClass;
  }

  static Db create(Ruby runtime, RocksDB db) {
    return new Db(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Db"), db);
  }

  private void internalClose() {
    db.close();
  }

  @Override
  public void close() {
    internalClose();
  }

  @JRubyMethod(name = "close")
  public IRubyObject closeRb(ThreadContext ctx) {
    internalClose();
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject put(ThreadContext ctx, IRubyObject key, IRubyObject value) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    if (value.isNil()) {
      throw ctx.runtime.newArgumentError("value can not be nil");
    }
    try {
      db.put(key.asString().getBytes(), value.asString().getBytes());
      return ctx.runtime.getNil();
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }

  @JRubyMethod
  public IRubyObject get(ThreadContext ctx, IRubyObject key) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    try {
      byte[] value = db.get(key.asString().getBytes());
      if (value == null) {
        return ctx.runtime.getNil();
      } else {
        return ctx.runtime.newString(new ByteList(value));
      }
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }

  @JRubyMethod
  public IRubyObject delete(ThreadContext ctx, IRubyObject key) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    try {
      db.remove(key.asString().getBytes());
      return ctx.runtime.getNil();
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }

  @JRubyMethod
  public IRubyObject batch(ThreadContext ctx, Block block) {
    try (WriteBatch batch = new WriteBatch()) {
      block.yield(ctx, Batch.create(ctx.runtime, batch));
      try (WriteOptions options = new WriteOptions()) {
        db.write(options, batch);
        return ctx.runtime.getNil();
      } catch (RocksDBException rdbe) {
        throw RocksDb.createError(ctx.runtime, rdbe);
      }
    }
  }

  @JRubyMethod
  public IRubyObject snapshot(ThreadContext ctx, Block block) {
    Snapshot snapshot = Snapshot.create(ctx.runtime, db);
    if (block.isGiven()) {
      block.yield(ctx, snapshot);
      snapshot.close(ctx);
      return ctx.runtime.getNil();
    } else {
      return snapshot;
    }
  }

  @JRubyMethod(optional = 1)
  public IRubyObject each(ThreadContext ctx, IRubyObject[] args, Block block) {
    RubyHash scanOptions = args.length > 0 ? args[0].convertToHash() : null;
    Iterator cursor = Iterator.create(ctx.runtime, db, new ReadOptions(), scanOptions);
    if (block.isGiven()) {
      cursor.each(ctx, block);
    }
    return cursor;
  }

  @JRubyMethod(optional = 1)
  public IRubyObject flush(ThreadContext ctx, IRubyObject[] args) {
    try (FlushOptions flushOptions = new FlushOptions()) {
      if (args.length > 0) {
        RubyHash options = args[0].convertToHash();
        IRubyObject waitOption = options.fastARef(ctx.runtime.newSymbol("wait"));
        if (waitOption != null) {
          flushOptions.setWaitForFlush(waitOption.isTrue());
        }
      }
      db.flush(flushOptions);
      return ctx.runtime.getNil();
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }

  @JRubyMethod(name = "compact_range", optional = 1)
  public IRubyObject compactRange(ThreadContext ctx, IRubyObject[] args) {
    byte[] from = null;
    byte[] to = null;
    if (args.length > 0) {
      RubyHash options = args[0].convertToHash();
      RubySymbol fromKey = ctx.runtime.newSymbol("from");
      RubySymbol toKey = ctx.runtime.newSymbol("to");
      boolean hasFrom = options.has_key_p(fromKey).isTrue();
      boolean hasTo = options.has_key_p(toKey).isTrue();
      if (hasFrom ^ hasTo) {
        throw ctx.runtime.newArgumentError("Either none or both of :from and :to must be given");
      } else if (hasFrom && hasTo) {
        from = options.fastARef(fromKey).asString().getBytes();
        to = options.fastARef(toKey).asString().getBytes();
      }
    }
    try {
      if (from != null) {
        db.compactRange(from, to);
      } else {
        db.compactRange();
      }
      return ctx.runtime.getNil();
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }
}
