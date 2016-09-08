# encoding: utf-8

module RocksDb
  # @private
  module LazyEnumerable
    include Enumerable

    def map(&transform)
      LazyMap.new(self, &transform)
    end

    def select(&filter)
      LazySelect.new(self, &filter)
    end
  end

  # @private
  class LazyMap
    include LazyEnumerable

    def initialize(enum, &transform)
      @enum = enum
      @transform = transform
    end

    def each(&block)
      if block
        @enum.each do |element|
          block.call(@transform.call(element))
        end
      end
      self
    end
  end

  # @private
  class LazySelect
    include LazyEnumerable

    def initialize(enum, &filter)
      @enum = enum
      @filter = filter
    end

    def each(&block)
      if block
        @enum.each do |element|
          block.call(element) if @filter.call(element)
        end
      end
      self
    end
  end
end
