package io.burt.rocksdb;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.RubyClass;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;

@JRubyClass(name = "RocksDb::Db")
public class Db {
  static RubyClass install(Ruby runtime, RubyModule parentModule) {
    RubyClass dbClass = parentModule.defineClassUnder("Db", runtime.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    dbClass.defineAnnotatedMethods(Db.class);
    return dbClass;
  }
}
