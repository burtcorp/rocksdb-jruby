# encoding: utf-8

require 'rocks_db/ext/rocks_db.jar'

Java::IoBurtRocksdb::RocksDbLibrary.new.load(JRuby.runtime, false)

require 'rocks_db/version'
require 'rocks_db/lazy_enumerable'

module RocksDb
  class Iterator
    include LazyEnumerable
  end
end

