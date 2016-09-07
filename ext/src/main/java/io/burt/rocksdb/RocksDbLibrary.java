package io.burt.rocksdb;

import org.jruby.Ruby;
import org.jruby.RubyModule;
import org.jruby.RubyClass;
import org.jruby.RubyString;
import org.jruby.RubyNil;
import org.jruby.RubyBoolean;
import org.jruby.RubyHash;
import org.jruby.runtime.load.Library;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.Block;
import org.jruby.runtime.Visibility;
import org.jruby.anno.JRubyModule;
import org.jruby.anno.JRubyMethod;

public class RocksDbLibrary implements Library {
  public void load(Ruby ruby, boolean wrap) {
  }
}
