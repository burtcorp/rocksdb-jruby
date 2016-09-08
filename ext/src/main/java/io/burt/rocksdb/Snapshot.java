package io.burt.rocksdb;

import org.rocksdb.ReadOptions;
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
@JRubyClass(name = "RocksDb::Snapshot")
public class Snapshot extends RubyObject {
  private final RocksDB db;
  private final org.rocksdb.Snapshot snapshot;
  private final ReadOptions options;

  public Snapshot(Ruby runtime, RubyClass cls, RocksDB db) {
    super(runtime, cls);
    this.db = db;
    this.snapshot = db.getSnapshot();
    this.options = new ReadOptions();
    this.options.setSnapshot(snapshot);
  }

  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass snapshotClass = parentModule.defineClassUnder("Snapshot", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    snapshotClass.defineAnnotatedMethods(Snapshot.class);
    return snapshotClass;
  }

  static Snapshot create(Ruby runtime, RocksDB db) {
    return new Snapshot(runtime, (RubyClass) runtime.getClassFromPath("RocksDb::Snapshot"), db);
  }

  @JRubyMethod
  public IRubyObject close(ThreadContext ctx) {
    snapshot.close();
    options.close();
    db.releaseSnapshot(snapshot);
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject get(ThreadContext ctx, IRubyObject key) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    try {
      byte[] value = db.get(options, key.asString().getBytes());
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
}
