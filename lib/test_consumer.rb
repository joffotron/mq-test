#!/usr/bin/env ruby
# encoding: utf-8

require 'amqp'

AMQP_OPTS = {:host => '127.0.0.1'}

class TestConsumer

  attr_reader :terminated

  def initialize(channel, queue_name = "rails.helloworld")
    @queue_name = queue_name
    @channel    = channel
    @channel.on_error(&method(:handle_channel_exception))
  end

  def start
    @queue = @channel.queue(@queue_name, :durable => true, :arguments => {'x-ha-policy''' => 'all'})
    @queue.subscribe(:ack => true) do |header, payload|
      handle_message(header, payload)
      header.ack
      puts "Message ack'd"
    end
  end

  def handle_message(header, payload)
    puts "Received a message: #{payload}, content_type = #{header.content_type}"
    sleep 0.5
    puts "Processed message #{payload}"
  end

  def handle_channel_exception(channel, channel_close)
    puts "Oops... a channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
    raise
  end
end

# ======================================================================================================================

EventMachine.run do
  @connection = AMQP.connect(AMQP_OPTS)
  channel     = AMQP::Channel.new(@connection)
  channel.prefetch 1

  puts "Got connection, starting consumer"
  consumer = TestConsumer.new(channel)
  consumer.start
  puts "Consumer started"

  Signal.trap('INT') do
    puts "INT received. Shutting down..."
    @connection.close { EventMachine.stop { exit } }
  end
end

puts "Done"
