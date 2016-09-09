# encoding: utf-8

module RocksDb
  #
  class Db
    # @!method close
    #   Closes the database handle.
    #
    #   This should be done when you are done using the database.
    #
    #   @return [nil]

    # @!method put(key, value)
    #   Puts a value into the database
    #
    #   @return [nil]

    # @!method get(key)
    #   Retrieves a value from the database
    #
    #   @return [String] the value

    # @!method delete(key)
    #   Deletes a value from the database
    #
    #   @return [nil]

    # @!method batch(&block)
    #   Yields a batch object that can be used to do multiple mutations as one
    #   atomic operation
    #
    #   Batching can be used both to perform multiple operations that must be
    #   performed atomically, or to speed up bulk loads.
    #
    #   @see RocksDb::Batch
    #   @yieldparam [RocksDb::Batch] batch the batch
    #   @return [nil]

    # @!method snapshot
    #   Returns a view of the database as it looks at this point in time
    #
    #   Mutations that happen after the snapshot has been created are not
    #   visible when using the snapshot (e.g. {RocksDb::Snapshot#get} will not
    #   return values that are written after the snaphot was created).
    #
    #   The snapshot must be closed using {RocksDb::Snapshot#close} when you
    #   are done with it.
    #
    #   When a block is given the snapshot is automatically closed when the
    #   block returns.
    #
    #   @see RocksDb::Snapshot
    #   @return [nil, RocksDb::Snapshot] a snapshot or nil if a block is given

    # @!method each(options={}, &block)
    #   Iterate over all or a subset of all keys and values.
    #
    #   Without any options this will yield every key and value in the database
    #   in order. Most of the time you want to specify the `:from` and/or `:to`
    #   and/or `:limit` to control which portion of the database that will be
    #   iterated over.
    #
    #   You can also iterate in reverse order by setting the `:reverse` option
    #   to `true`. Note that when you iterate in reverse order, the key specifed
    #   in `:from` will still be the first key yielded, and that the `:to` key
    #   should be a key _before_ the `:from` key in regular order.
    #
    #   @param [Hash] options
    #   @option options [String] :from (nil) the key to start the iteration from
    #   @option options [String] :to (nil) the key to end the iteration at
    #   @option options [Integer] :limit (nil) the maximum number of keys to
    #     iterate over. Negative values are ignored.
    #   @option options [true, false] :reverse (false) whether or not to iterate
    #     in reverse order
    #   @yieldparam [String] key a key
    #   @yieldparam [String] value a value
    #   @return [Enumerable] an enumerable of the same keys and values as the
    #     call would have yielded if a block was given

    # @!method compact_range(options={})
    #   Compact the whole, or a range of the keyspace
    #
    #   @param [Hash] options
    #   @option options [String] :from (nil) the start of the range to compact
    #   @option options [String] :to (nil) the end of the range to compact
    #   @return [nil]
  end
end
