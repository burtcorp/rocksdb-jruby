package io.burt.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

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
@JRubyClass(name = "RocksDb::Db")
public class Db extends RubyObject {
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

  static IRubyObject create(Ruby runtime, RocksDB db) {
    return new Db(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Db"), db);
  }

  @JRubyMethod
  public IRubyObject close(ThreadContext ctx) {
    db.close();
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
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("RocksDb::Error");
      throw ctx.runtime.newRaiseException(errorClass, rdbe.getMessage());
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
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("RocksDb::Error");
      throw ctx.runtime.newRaiseException(errorClass, rdbe.getMessage());
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
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("RocksDb::Error");
      throw ctx.runtime.newRaiseException(errorClass, rdbe.getMessage());
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
        RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("RocksDb::Error");
        throw ctx.runtime.newRaiseException(errorClass, rdbe.getMessage());
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
    byte[] from = null;
    byte[] to = null;
    int limit = -1;
    boolean reverse = false;
    if (args.length > 0 && !args[0].isNil()) {
      RubyHash scanOptions = args[0].convertToHash();
      IRubyObject scanFrom = scanOptions.fastARef(ctx.runtime.newSymbol("from"));
      if (scanFrom != null && !scanFrom.isNil()) {
        from = scanFrom.asString().getBytes();
      }
      IRubyObject scanTo = scanOptions.fastARef(ctx.runtime.newSymbol("to"));
      if (scanTo != null && !scanTo.isNil()) {
        to = scanTo.asString().getBytes();
      }
      IRubyObject scanLimit = scanOptions.fastARef(ctx.runtime.newSymbol("limit"));
      if (scanLimit != null && !scanLimit.isNil()) {
        limit = (int) scanLimit.convertToInteger().getLongValue();
      }
      IRubyObject scanReverse = scanOptions.fastARef(ctx.runtime.newSymbol("reverse"));
      reverse = scanReverse != null && scanReverse.isTrue();
    }
    Cursor scanner = Cursor.create(ctx.runtime, db, from, to, limit, reverse);
    if (block.isGiven()) {
      scanner.each(ctx, block);
    }
    return scanner;
  }
}
