require_relative 'spec_helper'

describe Volcanic::Cache::Cache do
  let(:clock) { double('clock', now: 20) }
  let(:max_size) { 3 }
  subject(:instance) { Volcanic::Cache::Cache.new(max_size: max_size).tap { |c| c._clock = clock } }
  let(:source_values) { [1, 2, 3, 4, 5] }
  let(:source_mock) { double('source') } 
  let(:key) { :key }

  before { allow(source_mock).to receive(:next).and_return(*source_values) }

  describe 'fetching' do
    context 'when the key has not been stored before' do
      let(:fetched) { instance.fetch(key) { source_mock.next} }

      it('does not contain the key') \
        { expect(instance.key?(key)).to be false }

      it('requests the value from the source only once') do
        expect(source_mock).to receive(:next).once
        expect(instance.fetch(key) { source_mock.next}).to eq(source_values[0])
        expect(instance.fetch(key) { source_mock.next}).to eq(source_values[0])
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
