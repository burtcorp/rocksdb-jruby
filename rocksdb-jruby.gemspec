# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'rocks_db/version'

Gem::Specification.new do |s|
  s.name = 'rocksdb-jruby'
  s.version = RocksDb::VERSION
  s.author = 'Theo Hultberg'
  s.email = 'theo@burtcorp.com'
  s.summary = 'RocksDB for JRuby'
  s.description = 'RocksDB bindings for JRuby'
  s.platform = 'java'

  s.files = Dir['lib/**/*.rb', 'lib/**/*.jar', 'README.md', '.yardopts']
end
