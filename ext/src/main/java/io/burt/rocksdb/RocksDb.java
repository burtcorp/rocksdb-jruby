package io.burt.rocksdb;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.ThreadContext;
import org.jruby.anno.JRubyModule;
import org.jruby.anno.JRubyMethod;

@JRubyModule(name = "RocksDb")
public class RocksDb {
  static RubyModule install(Ruby runtime) {
    RubyModule rocksDbModule = runtime.defineModule("RocksDb");
    rocksDbModule.defineAnnotatedMethods(RocksDb.class);
    return rocksDbModule;
  }

  @JRubyMethod(module = true, required = 1)
  public static IRubyObject open(ThreadContext ctx, IRubyObject recv, IRubyObject path) {
    return ctx.runtime.getNil();
  }
}
