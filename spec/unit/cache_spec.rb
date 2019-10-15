require_relative 'spec_helper'

describe Volcanic::Cache::Cache do
  let(:clock) { double('clock', now: 20) }
  let(:max_size) { 3 }
  subject(:instance) \
    { Volcanic::Cache::Cache.new(max_size: max_size).tap { |c| c._clock = clock } }
  let(:source_values) { [1, 2, 3, 4, 5] }
  let(:source_mock) { double('source') }
  let(:key) { :key }

  before { allow(source_mock).to receive(:next).and_return(*source_values) }

  describe 'fetching' do
    context 'when the key has not been stored before' do
      let(:fetched) { instance.fetch(key) { source_mock.next } }

      it('does not contain the key') \
        { expect(instance.key?(key)).to be false }

      it('requests the value from the source only once') do
        expect(source_mock).to receive(:next).once
        expect(instance.fetch(key) { source_mock.next }).to eq(source_values[0])
        expect(instance.fetch(key) { source_mock.next }).to eq(source_values[0])
      end

      it('returns the value') \
        { expect(fetched).to eq(source_values[0]) }

      it('increases the size') do
        expect(instance.size).to eq(0)
        fetched
        expect(instance.size).to eq(1)
      end

      it('returns the correct value') do
        zero = instance.fetch(:zero) { source_mock.next }
        one = instance.fetch(:one) { source_mock.next }
        expect(zero).to eq(source_values[0])
        expect(one).to eq(source_values[1])
      end

      it('contain the key after fetching') do
        fetched
        expect(instance.key?(key)).to be true
      end

      it('sets the ttl correctly with expire_in') do
        instance.fetch(key, expire_in: 20) { source_mock.next }
        expect(instance.ttl_for(key)).to eq(20)
      end

      it('sets the ttl correctly with expire_at') do
        instance.fetch(key, expire_at: 1000) { source_mock.next }
        expect(instance.ttl_for(key)).to eq(1000 - clock.now)
      end

      it('sets the ttl correctly using the default ttl') do
        instance.fetch(key) { source_mock.next }
        expect(instance.ttl_for(key)).to eq(instance.default_expiry)
      end

      context 'when no block is provided' do
        it('raises a cache miss') \
          { expect { instance.fetch(key) }.to raise_error(Volcanic::Cache::CacheMissError) }
      end
    end

    context 'when the value has expired' do
      before do
        allow(clock).to receive(:now).and_return(0, 6)
        instance.fetch(key, expire_in: 5) { source_mock.next }
      end

      it('refreshes the value') do
        expect(instance.fetch(key, expire_in: 5) { source_mock.next }).to eq(source_values[1])
      end

      it('evicts the old values') do
        expect(instance.size).to eq(1)
        instance.fetch(key, expire_in: 5) { source_mock.next }
        expect(instance.size).to eq(1)
        expect(instance.key?(key)).to be true
      end

      context 'when no block is provided' do
        it('raises a cache miss') \
          { expect { instance.fetch(key) }.to raise_error(Volcanic::Cache::CacheMissError) }
      end
    end
  end

  context 'when the cache is full' do
    before do
      instance.fetch(:zero, expire_in: 1) { source_mock.next }
      instance.fetch(:one, expire_in: 2) { source_mock.next }
      instance.fetch(:two, expire_in: 3) { source_mock.next }
    end

    it 'does not increase the size' do
      expect(instance.size).to eq 3
      instance.fetch(:three, expire_in: 4) { source_mock.next }
      expect(instance.size).to eq 3
    end

    it 'evicts the closest values to expiring' do
      instance.fetch(:three, expire_in: 4) { source_mock.next }
      expect(instance.key?(:zero)).to be false
    end

    it 'does not store the new item if it would be the first choice to evict' do
      instance.fetch(:three, expire_in: 0) { source_mock.next }
      expect(instance.key?(:three)).to be false
    end
  end

  context 'forced eviction' do
    before do
      instance.fetch(:zero, expire_in: 1) { source_mock.next }
      instance.fetch(:one, expire_in: 2) { source_mock.next }
      instance.fetch(:two, expire_in: 3) { source_mock.next }
    end

    it 'evicts the correct value immediately' do
      expect(instance.size).to eq(3)
      instance.evict!(:one)
      expect(instance.size).to eq(2)
      expect(instance.key?(:one)).to be false
    end
  end

  context 'put' do
    let(:new_value) { double(:new_value) }
    let(:expiry) { 2000 } # randomly chosen large number

    shared_examples 'the value is stored with the correct expiry' do
      before { instance.put(key, expire_at: expiry) { new_value } }

      it('stores the provided value') { expect(instance.fetch(key)).to be new_value }
      it('has the correct ttl') { expect(instance.ttl_for(key)).to eq(expiry - clock.now) }

      context 'when no block is provided' do
        it('raises an argument error') \
          { expect { instance.put(key) }.to raise_error(ArgumentError) }
      end
    end

    context 'without a value for the key' do
      it_behaves_like 'the value is stored with the correct expiry'
    end

    context 'with a value for the key already in the cache' do
      before { instance.fetch(:key, expire_at: 1000) { :old_value } }

      it_behaves_like 'the value is stored with the correct expiry'
    end
  end

  context 'extending the ttl' do
    context "when the key doesn't exist" do
      it('raises a cache miss error') \
        { expect { instance.update_ttl_for(:missing) }.to raise_error(Volcanic::Cache::CacheMissError) }
    end

    context 'when the key exists' do
      before do
        instance.put(:key, expire_at: 20) { source_mock }
        allow(clock).to receive(:now).and_return(0)
      end

      context 'and no block is provided' do
        it 'extends the expiry time' do
          instance.update_ttl_for(:key, expire_at: 50)
          expect(instance.ttl_for(:key)).to be(50)
        end
      end

      context 'and a block is provided' do
        before { instance.update_ttl_for(:key, expire_at: 50, &condition) }

        context 'and the block returns true' do
          let(:condition) { ->(_) { true } }
          it 'extends the expiry time' do
            expect(instance.ttl_for(:key)).to be(50)
          end
        end

        context 'and the block returns false' do
          let(:condition) { ->(_) { false } }
          it 'does not extend the expiry time' do
            expect(instance.ttl_for(:key)).to be(20)
          end
        end
      end
    end
    #update_ttl_for(key, expire_in: expire_in, expire_at: expire_at, immortal: immortal, &condition)
  end

  context 'garbage collection' do
    let(:max_size) { 10 }

    before do
      allow(clock).to receive(:now).and_return(0)
      instance.fetch(:zero, expire_in: 1) { source_mock.next }
      instance.fetch(:one, expire_in: 2) { source_mock.next }
      instance.fetch(:two, expire_in: 3) { source_mock.next }
      allow(clock).to receive(:now).and_return(20)
      instance.fetch(:three, expire_in: 1) { source_mock.next }
      instance.fetch(:four, expire_in: 2) { source_mock.next }
    end

    it 'removes all expired values' do
      instance.gc!
      expect(instance.size).to eq(2)
      %i(zero one two).each { |key| expect(instance.key?(key)).to be false }
      expect(instance.key?(:three)).to be true
      expect(instance.key?(:four)).to be true
    end
  end
end
