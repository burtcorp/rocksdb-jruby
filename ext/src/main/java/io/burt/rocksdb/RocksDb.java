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
import org.jruby.exceptions.RaiseException;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

@JRubyModule(name = "RocksDb")
public class RocksDb {
  static RubyModule install(Ruby runtime) {
    RubyModule rocksDbModule = runtime.defineModule("RocksDb");
    rocksDbModule.defineAnnotatedMethods(RocksDb.class);
    return rocksDbModule;
  }

  static RaiseException createError(Ruby runtime, RocksDBException rdbe) {
    String msg = rdbe.getMessage();
    String errorType = "RocksDb::Error";
    if (msg.startsWith("IO error:")) {
      errorType = "RocksDb::IoError";
    } else if (msg.contains("does not exist (create_if_missing") || msg.contains("exists (error_if_exists")) {
      errorType = "RocksDb::InvalidArgumentError";
    }
    RubyClass errorClass = (RubyClass) runtime.getClassFromPath(errorType);
    throw runtime.newRaiseException(errorClass, rdbe.getMessage());
  }

  @JRubyMethod(module = true, required = 1, optional = 1)
  public static IRubyObject open(ThreadContext ctx, IRubyObject recv, IRubyObject[] args, Block block) {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setErrorIfExists(false);
    if (args.length > 1) {
      RubyHash openOptions = args[1].convertToHash();
      IRubyObject createIfMissing = openOptions.fastARef(ctx.runtime.newSymbol("create_if_missing"));
      options.setCreateIfMissing(createIfMissing != null && createIfMissing.isTrue());
      IRubyObject errorIfExists = openOptions.fastARef(ctx.runtime.newSymbol("error_if_exists"));
      options.setErrorIfExists(errorIfExists != null && errorIfExists.isTrue());
    }
    try {
      RocksDB rocksDb = RocksDB.open(options, args[0].asJavaString());
      Db db = Db.create(ctx.runtime, rocksDb);
      if (block.isGiven()) {
        try {
          block.yield(ctx, db);
        } finally {
          db.close();
        }
        return ctx.runtime.getNil();
      } else {
        return db;
      }
    } catch (RocksDBException rdbe) {
      throw createError(ctx.runtime, rdbe);
    }
  }
}
