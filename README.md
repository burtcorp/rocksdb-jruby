# RocksDB bindings for JRuby

[![Build Status](https://travis-ci.org/burtcorp/rocksdb-jruby.png?branch=master)](https://travis-ci.org/burtcorp/rocksdb-jruby)

_If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can find the readme for a specific version via the release tags ([here is an example](https://github.com/burtcorp/rocksdb-jruby/releases/tag/v0.1.0))._

## Installation

rocksdb-jruby is available from RubyGems. Either install it manually:

```
$ gem install rocksdb-jruby
```

or add this to your `Gemfile`:

```ruby
gem 'rocksdb-jruby', require: 'rocks_db'
```

## Usage

```ruby
require 'rocks_db'

db = RocksDb.open('path/to/database')

# basic key operations
db.put('foo', 'bar')
puts db.get('foo') # => 'bar'
db.delete('foo')

# iterating over a range of keys
10.times { |i| db.put("foo#{i.to_s.rjust(2, '0')}", i.to_s) }
db.each(from: 'foo', to: 'foo08') do |key, value|
  puts "#{key} => #{value}"
end

# batch mutations
db.batch do |batch|
  batch.put('foo', 'bar')
  batch.delete('bar')
end

# read from a snapshot
db.put('foo', 'bar')
snapshot = db.snapshot
db.put('foo', 'baz')
puts snapshot.get('foo') # => 'bar'
puts db.get('foo') # => 'baz'

# compactions
db.compact_range(from: 'foo', to: 'foo08')
```

## How to build and run the tests

The best place to see how to build and run the tests is to look at the `.travis.yml` file, but if you just want to get going run:

```
$ bundle install
$ bundle exec rake
```

# Copyright

© 2016 Burt AB, see LICENSE.txt (BSD 3-Clause).
