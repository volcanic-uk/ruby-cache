require 'set'
require 'forwardable'

module Volcanic::Cache
  class Cache
    class CacheMissError < RuntimeError; end

    # Provide support for using the cache as a singleton
    extend SingleForwardable
    @_singleton_mutex = Mutex.new
    @_singleton_settings = {}
    def_delegators :instance, :fetch, :evict, :key?, :gc!, :size

    # injectable clock for testing
    attr_writer :_clock

    attr_accessor :default_expiry, :max_size

    def initialize(max_size: 1000, default_expiry: 60)
      @key_values = {}
      @expiries = Hash.new { |h, k| h[k] = Set.new }
      @mutex = Mutex.new
      @max_size = max_size
      @default_expiry = default_expiry

      @_clock = Time
    end

    def fetch(key, expire_in: nil, expire_at: nil, &blk)
      expiry = expire_at || now + (expire_in || @default_expiry)
      @mutex.synchronize { in_mutex_fetch(key, expiry: expiry, &blk) }
    end

    def evict!(key)
      @mutex.synchronize { in_mutex_expire!(key) }
    end

    def size
      @mutex.synchronize { @key_values.size }
    end

    def key?(key)
      @mutex.synchronize { in_mutex_key?(key) }
    end

    def gc!
      @mutex.synchronize { in_mutex_gc! }
    end

    # support the Singleton options
    class << self
      def instance
        @_singleton_mutex.synchronize { @_singleton_instance ||= new(**@_singleton_settings) }
      end

      # A reset option for testing... replace the existing singleton with a new one with the same settings
      def _reset_instance
        @_singleton_mutex.synchronize { @_singleton_instance = new(**@_singleton_settings) }
      end

      def max_size=(value)
        @_singleton_settings[:max_size] = value
        instance.max_size = value
      end

      def default_expiry=(value)
        @_singleton_settings[:default_expiry] = value
        instance.default_expiry = value
      end
    end


    private

    # Allow a clock to be injected for testing
    def now
      @_clock.now.to_i
    end

    ################################################
    # do not call these methods outside the mutex! #
    ################################################

    def in_mutex_expire!(key)
      expiry, = @key_values[key]
      unless expiry.nil?
        @expiries[expiry].delete(key)
        @expiries[expiry]
        @key_values.delete(key)
      end
    end

    def in_mutex_fetch(key, expiry:)
      in_mutex_retrieve_local_with_expire(key)
    rescue CacheMissError
      value = yield
      in_mutex_store(key, value, expiry)
      value
    end

    def in_mutex_retrieve_local_with_expire(key)
      expiry, value = @key_values[key]
      if expiry.nil?
        raise CacheMissError
      else
        if expiry < now
          in_mutex_expire_all_by_time(expiry)
          raise CacheMissError
        else
          value
        end
      end
    end

    def in_mutex_key?(key)
      in_mutex_retrieve_local_with_expire(key)
      true
    rescue CacheMissError
      false
    end

    def in_mutex_store(key, value, expiry)
      @key_values[key] = [expiry, value]
      @expiries[expiry].add key
      in_mutex_expire_oldest if @key_values.size > @max_size
    end

    def in_mutex_expire_oldest
      in_mutex_expire_all_by_time(@expiries.keys.min)
    end

    def in_mutex_gc!
      time = now
      @expiries.keys.each { |expiry| in_mutex_expire_all_by_time(expiry) if expiry < time }
    end

    def in_mutex_expire_all_by_time(expiry)
      @expiries[expiry].each { |key| in_mutex_expire!(key) }
    end
  end
end
