# frozen_string_literal: true
require_relative 'helper'
require 'sidekiq/fetch'
require 'sidekiq/api'

class TestFetcher < Minitest::Test
  describe 'fetcher' do
    before do
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.rpush('queue:basic', 'msg')
      end
    end

    it 'retrieves' do
      fetch = Sidekiq::BasicFetch.new(:queues => ['basic', 'bar'])
      uow = fetch.retrieve_work
      refute_nil uow
      assert_equal 'basic', uow.queue_name
      assert_equal 'msg', uow.job
      q = Sidekiq::Queue.new('basic')
      assert_equal 0, q.size
      uow.requeue
      assert_equal 1, q.size
      assert_nil uow.acknowledge
    end

    it 'retrieves with strict setting' do
      fetch = Sidekiq::BasicFetch.new(:queues => ['basic', 'bar'], :strict => true)
      cmd = fetch.queues_cmd
      assert_equal cmd, ['queue:basic', 'queue:bar', Sidekiq::BasicFetch::TIMEOUT]
    end

    describe 'when duplicate queue names' do
      it 'raises an argument error' do
        assert_raises(ArgumentError) { Sidekiq::BasicFetch.new(:queues => ['basic', 'bar', 'bar'], :strict => true) }
        assert_raises(ArgumentError) { Sidekiq::BasicFetch.new(:queues => ['bar', ['bar', 2]]) }
      end
    end

    it 'retrieves with queues with weights' do
      fetch = Sidekiq::BasicFetch.new(:queues => ['bar', ['foo', 3]])
      cmd = fetch.queues_cmd
      # Sorting to remove the randomness introduced by queues_cmd by shuffling the queue names
      cmd_sorted = [cmd[0..1].sort, cmd[-1]].flatten
      assert_equal cmd_sorted, ['queue:bar', 'queue:foo', Sidekiq::BasicFetch::TIMEOUT]
    end

    it 'bulk requeues' do
      q1 = Sidekiq::Queue.new('foo')
      q2 = Sidekiq::Queue.new('bar')
      assert_equal 0, q1.size
      assert_equal 0, q2.size
      uow = Sidekiq::BasicFetch::UnitOfWork
      Sidekiq::BasicFetch.bulk_requeue([uow.new('fuzzy:queue:foo', 'bob'), uow.new('fuzzy:queue:foo', 'bar'), uow.new('fuzzy:queue:bar', 'widget')], {:queues => []})
      assert_equal 2, q1.size
      assert_equal 1, q2.size
    end

  end
end
