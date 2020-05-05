require 'set'
require 'forwardable'
require_relative 'errors.rb'

module Volcanic::Cache
  class Cache
    IMMORTAL_TTL = 3_155_760_000 # 100 years in seconds

    # Provide support for using the cache as a singleton
    extend SingleForwardable
    @_singleton_mutex = Mutex.new
    @_singleton_settings = {}
    def_delegators :instance, :fetch, :evict, :key?, :gc!, :size

    # injectable clock for testing
    attr_writer :_clock

    attr_accessor :default_expiry, :max_size

    def initialize(max_size: 1000, default_expiry: 60, cache_nil: true)
      @key_values = {}
      @expiries = Hash.new { |h, k| h[k] = Set.new }
      @mutex = Mutex.new
      @cv_waiter = ConditionVariable.new
      @mutex_keys = {}
      @max_size = max_size
      @default_expiry = default_expiry
      @cache_nil = cache_nil

      @_clock = Time
    end

    def fetch(key, expire_in: nil, expire_at: nil, immortal: false, &blk)
      with_mutex_for(key) do
        unsafe_fetch key, expire_in: expire_in, expire_at: expire_at,
                     immortal: immortal, &blk
      end
    end

    ############################################################
    # This is a dangerous method because it is not thread safe #
    # This should only (normally) be used when inside a block  #
    # that is executed inside the appropriate mutex            #
    ############################################################
    def unsafe_fetch(key, expire_in: nil, expire_at: nil, immortal: false, &blk)
      expiry = calculate_expiry(expire_in: expire_in, expire_at: expire_at, immortal: immortal)
      in_mutex_fetch(key, expiry: expiry, &blk)
    end

    def put(key, expire_in: nil, expire_at: nil, immortal: false, &blk)
      with_mutex_for(key) do
        unsafe_put key, expire_in: expire_in, expire_at: expire_at, immortal: immortal, &blk
      end
    end

    ############################################################
    # This is a dangerous method because it is not thread safe #
    # This should only (normally) be used when inside a block  #
    # that is executed inside the appropriate mutex            #
    ############################################################
    def unsafe_put(key, expire_in: nil, expire_at: nil, immortal: false)
      raise ArgumentError.new("Attempted to put #{key} without providing a block") \
        unless block_given?
      expiry = calculate_expiry(expire_in: expire_in, expire_at: expire_at, immortal: immortal)
      in_mutex_store(key, yield, expiry)
    end

    def evict!(key)
      with_mutex_for(key) { in_mutex_expire!(key) }
    end

    def size
      @mutex.synchronize { @key_values.size }
    end

    def key?(key)
      with_mutex_for(key) { in_mutex_key?(key) }
    end

    ############################################################
    # This is a dangerous method because it is not thread safe #
    # This should only (normally) be used when inside a block  #
    # that is executed inside the appropriate mutex            #
    ############################################################
    def unsafe_key?(key)
      in_mutex_key?(key)
    end

    def ttl_for(key)
      with_mutex_for(key) { in_mutex_ttl_for(key) }
    end

    def gc!
      @mutex.synchronize do
        @cv_waiter.wait(@mutex) unless @mutex_keys.empty?
        in_mutex_gc!
      end
    end

    def update_ttl_for(key, expire_in: nil, expire_at: nil, immortal: nil, &condition)
      with_mutex_for(key) do
        in_mutex_update_ttl_for key, expire_in: expire_in, expire_at: expire_at,
                                immortal: immortal, &condition
      end
    end

    ############################################################
    # This is a dangerous method because it is not thread safe #
    # This should only (normally) be used when inside a block  #
    # that is executed inside the appropriate mutex            #
    ############################################################
    def unsafe_update_ttl_for(key, expire_in: nil, expire_at: nil, immortal: nil, &condition)
      in_mutex_update_ttl_for key, expire_in: expire_in, expire_at: expire_at,
                              immortal: immortal, &condition
    end

    # support the Singleton options
    class << self
      def instance
        @_singleton_mutex.synchronize { @_singleton_instance ||= new(**@_singleton_settings) }
      end

      # A reset option for testing...
      # replace the existing singleton with a new one with the same settings
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

    def with_mutex_for(key)
      mutex = accessors = nil
      @mutex.synchronize do
        mutex, accessors = *(@mutex_keys[key] ||= [Mutex.new, Set.new])
        accessors << Thread.current
      end
      result = mutex.synchronize { yield }
    ensure
      @mutex.synchronize do
        accessors.delete Thread.current
        if accessors.empty?
          @mutex_keys.delete(key)
          @cv_waiter.signal
        end
      end
      result
    end

    # Allow a clock to be injected for testing
    def now
      @_clock.now.to_i
    end

    def calculate_expiry(expire_at: nil, expire_in: nil, immortal: false)
      raise ArgumentError.new('Only one of immortal, expire_at or expire_in can be used') \
        if [expire_at.nil?, expire_in.nil?, !immortal].count(false) > 1
      expire_in = IMMORTAL_TTL if immortal
      expire_at || now + (expire_in || @default_expiry)
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
      raise CacheMissError unless block_given?
      value = yield
      in_mutex_store(key, value, expiry) if @cache_nil
      value
    end

    def in_mutex_ttl_for(key)
      in_mutex_retrieve_local_with_expire_and_ttl(key)[0]
    end

    def in_mutex_retrieve_local_with_expire(key)
      in_mutex_retrieve_local_with_expire_and_ttl(key)[1]
    end

    def in_mutex_retrieve_local_with_expire_and_ttl(key)
      expiry, value = @key_values[key]
      if expiry.nil?
        raise CacheMissError
      else
        if expiry < now
          in_mutex_expire_all_by_time(expiry)
          raise CacheMissError
        else
          [expiry - now, value]
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

    def in_mutex_update_ttl_for(key, expire_in: nil, expire_at: nil, immortal: nil, &_condition)
      value = in_mutex_retrieve_local_with_expire(key)
      if !block_given? || yield(value)
        expiry = calculate_expiry(expire_in: expire_in, expire_at: expire_at, immortal: immortal)
        @expiries[expiry].delete key
        @expiries[expiry].add key
        @key_values[key][0] = expiry
      end
    end
  end
end
