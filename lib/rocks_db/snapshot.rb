# encoding: utf-8

module RocksDb
  class Snapshot
    # @!method close
    #   Tells the database that it doesn't have to track this snapshot anymore
    #
    #   This should be done when you are done using the snapshot. Do not attempt
    #   to use the snapshot after having called this method.
    #
    #   @return [nil]

    # @!method get(key)
    #   Retrieves a value from the database
    #
    #   @return [String] the value

    # @!method each(options={}, &block)
    #   Iterate over all or a subset of all keys and values.
    #
    #   Please refer to {RocksDb::Db#each} for the full documentation.
    #
    #   @see RocksDb::Db#each
    #   @return [Enumerable] an enumerable of the same values as the
    #     call would have yielded to the block
  end
end
