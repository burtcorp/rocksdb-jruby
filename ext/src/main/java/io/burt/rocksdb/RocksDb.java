package io.burt.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.anno.JRubyModule;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@JRubyModule(name = "RocksDb")
public class RocksDb {
  static RubyModule install(Ruby runtime) {
    RubyModule rocksDbModule = runtime.defineModule("RocksDb");
    rocksDbModule.defineAnnotatedMethods(RocksDb.class);
    return rocksDbModule;
  }

  @JRubyMethod(module = true, required = 1, optional = 1)
  public static IRubyObject open(ThreadContext ctx, IRubyObject recv, IRubyObject[] args) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setErrorIfExists(false);
    if (args.length > 1) {
      RubyHash openOptions = (RubyHash) args[1];
      IRubyObject createIfMissing = openOptions.fastARef(ctx.runtime.newSymbol("create_if_missing"));
      options.setCreateIfMissing(createIfMissing != null && createIfMissing.isTrue());
      IRubyObject errorIfExists = openOptions.fastARef(ctx.runtime.newSymbol("error_if_exists"));
      options.setErrorIfExists(errorIfExists != null && errorIfExists.isTrue());
    }
    try {
      RocksDB db = RocksDB.open(options, args[0].asJavaString());
      return Db.create(ctx.runtime, db);
    } catch (RocksDBException rdbe) {
      RubyClass errorClass = (RubyClass) ctx.runtime.getClassFromPath("RocksDb::Error");
      throw ctx.runtime.newRaiseException(errorClass, rdbe.getMessage());
    }
  }
}
