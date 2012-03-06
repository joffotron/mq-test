#!/usr/bin/env ruby
# encoding: utf-8

require 'amqp'

AMQP_OPTS = {:host => '127.0.0.1'}

class TestConsumer

  def initialize(channel, queue_name = "rails.helloworld")
    @queue_name = queue_name
    @channel    = channel
    @channel.on_error(&method(:handle_channel_exception))
  end


  def start
    @queue = @channel.queue(@queue_name, :durable => true)
    @queue.subscribe(:ack =>true) do |metadata, payload|
      if @terminating
        @queue.unsubscribe
        @termination_block.call
      else
        handle_message(metadata, payload)
      end
    end
  end

  def stop(&blk)
    @terminating = true
    @termination_block = blk
  end

  def handle_message(metadata, payload)
    puts "Received a message: #{payload}, content_type = #{metadata.content_type}"
    sleep 2
    puts "Processed message #{payload}"
    puts metadata.ack
  end

  def handle_channel_exception(channel, channel_close)
    puts "Oops... a channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
    raise
  end

end

EventMachine.run do
  @connection = AMQP.connect(AMQP_OPTS)
  channel    = AMQP::Channel.new(@connection)
  puts "Got connection, starting consumer"
  consumer = TestConsumer.new(channel)
  consumer.start
  puts "Consumer started"

  Signal.trap('INT') do
    puts "INT received. Shutting down..."
    consumer.stop { @connection.close{ EventMachine.stop{ exit } } }
  end
  Signal.trap('TERM') do
    puts "TERM received. Shutting down..."
    consumer.stop { @connection.close{ EventMachine.stop{ exit } } }
  end
end

puts "Done"
