# encoding: utf-8

require 'rocks_db/ext/rocks_db.jar'

Java::IoBurtRocksdb::RocksDbLibrary.new.load(JRuby.runtime, false)

require 'rocks_db/version'
