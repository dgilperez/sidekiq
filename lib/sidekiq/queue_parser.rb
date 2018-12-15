# frozen_string_literal: true

module Sidekiq
  module QueueParser
    def parse_queues(opts, queues_and_weights)
      queues_and_weights.each { |queue_and_weight| parse_queue(opts, *queue_and_weight) }
    end

    def parse_queue(opts, queue, weight = nil)
      opts[:queues] ||= []
      raise ArgumentError, "queues: #{queue} cannot be defined twice" if opts[:queues].include?(queue)
      [weight.to_i, 1].max.times { opts[:queues] << queue }
      opts[:strict] = false if weight.to_i > 0
    end
  end
end
