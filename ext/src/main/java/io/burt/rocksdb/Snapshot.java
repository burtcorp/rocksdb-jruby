package io.burt.rocksdb;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
@JRubyClass(name = "RocksDb::Snapshot")
public class Snapshot extends RubyObject {
  private final RocksDB db;
  private final org.rocksdb.Snapshot snapshot;
  private final ReadOptions readOptions;

  public Snapshot(Ruby runtime, RubyClass cls, RocksDB db) {
    super(runtime, cls);
    this.db = db;
    this.snapshot = db.getSnapshot();
    this.readOptions = new ReadOptions();
    this.readOptions.setSnapshot(snapshot);
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
    readOptions.close();
    db.releaseSnapshot(snapshot);
    return ctx.runtime.getNil();
  }

  @JRubyMethod
  public IRubyObject get(ThreadContext ctx, IRubyObject key) {
    if (key.isNil()) {
      throw ctx.runtime.newArgumentError("key can not be nil");
    }
    try {
      byte[] value = db.get(readOptions, key.asString().getBytes());
      if (value == null) {
        return ctx.runtime.getNil();
      } else {
        return ctx.runtime.newString(new ByteList(value));
      }
    } catch (RocksDBException rdbe) {
      throw RocksDb.createError(ctx.runtime, rdbe);
    }
  }

  @JRubyMethod(optional = 1)
  public IRubyObject each(ThreadContext ctx, IRubyObject[] args, Block block) {
    RubyHash scanOptions = args.length > 0 ? args[0].convertToHash() : null;
    Iterator cursor = Iterator.create(ctx.runtime, db, readOptions, scanOptions);
    if (block.isGiven()) {
      cursor.each(ctx, block);
    }
    return cursor;
  }
}
