package io.burt.rocksdb;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
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
}