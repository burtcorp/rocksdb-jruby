package io.burt.rocksdb;

import org.rocksdb.WriteBatch;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@SuppressWarnings("serial")
@JRubyClass(name = "RocksDb::Batch")
public class Batch extends RubyObject {
  private final WriteBatch batch;

  public Batch(Ruby runtime, RubyClass cls, WriteBatch batch) {
    super(runtime, cls);
    this.batch = batch;
  }
  
  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass dbClass = parentModule.defineClassUnder("Batch", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    dbClass.defineAnnotatedMethods(Batch.class);
    return dbClass;
  }

  static IRubyObject create(Ruby runtime, WriteBatch batch) {
    return new Batch(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Batch"), batch);
  }
  
  @JRubyMethod
  public IRubyObject put(ThreadContext ctx, IRubyObject key, IRubyObject value) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    if (value.isNil()) {
      throw ctx.runtime.newArgumentError("value can not be nil");
    }
    batch.put(key.asString().getBytes(), value.asString().getBytes());
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject delete(ThreadContext ctx, IRubyObject key) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    batch.remove(key.asString().getBytes());
    return ctx.runtime.getNil();
  }
}
