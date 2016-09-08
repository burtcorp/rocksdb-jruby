# encoding: utf-8

require 'rocks_db/ext/rocks_db.jar'

Java::IoBurtRocksdb::RocksDbLibrary.new.load(JRuby.runtime, false)

require 'rocks_db/version'
require 'rocks_db/lazy_enumerable'

module RocksDb
  # @private
  class Iterator
    include LazyEnumerable
  end

  # @!method open(path, options={})
  #   Open and/or create a database
  #
  #   A database is a directory of files on disk, specified by a path to the
  #   directory. A database may only be opened by one process at a time, but
  #   within a process the database can safely be used concurrently by multiple
  #   threads.
  #
  #   @param [String] path the path to the database
  #   @param [Hash] options
  #   @option options [true, false] :create_if_missing (true) Whether or not to
  #     create the database if it does not already exist.
  #   @option options [true, false] :error_if_exists (false) Whether or not to
  #     throw an error when the database already exists.
  #   @raise [RocksDb::Error] raised when an internal error occurs, and when
  #     the `:error_if_exists` options is true and the database already exists.
  #   @return [RocksDb::Db] a database handle
end
