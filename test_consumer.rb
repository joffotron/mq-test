#!/usr/bin/env ruby
# encoding: utf-8

require 'amqp'

AMQP_NODE_1 = {:host => '127.0.0.1', :port => 5672}
AMQP_NODE_2 = {:host => '127.0.0.1', :port => 5673}

class TestConsumer

  def initialize(channel, queue_name = "rails.helloworld")
    @queue_name = queue_name
    @channel    = channel
    @queue = @channel.queue(@queue_name, :durable => true, :arguments => {'x-ha-policy' '' => 'all'})
  end

  def start
    @queue.subscribe(:ack => true) do |header, payload|
      handle_message(header, payload)
      header.ack
      puts "Message ack'd"
    end
  end

  def handle_message(header, payload)
    puts "Received a message: #{payload}, content_type = #{header.content_type}"
    sleep 0.1
    puts "Processed message #{payload}"
  end

end

# ======================================================================================================================

def next_server
  (@current_server == AMQP_NODE_1) ? AMQP_NODE_2 : AMQP_NODE_1
end

EventMachine.run do
  @current_server = AMQP_NODE_1

  connection = AMQP.connect(@current_server)

  channel    = AMQP::Channel.new(connection, 1, :auto_recovery => true)
  channel.prefetch 1

  consumer = TestConsumer.new(channel)

  connection.on_tcp_connection_loss do
    puts "Lost connection to #{@current_server[:host]}:#{@current_server[:port]}"
    @current_server = next_server
    connection.reconnect_to(@current_server)
  end

  connection.on_recovery do
    puts "Re-connected to #{@current_server[:host]}:#{@current_server[:port]}"
    #consumer.start
    #puts "Re-started consumer subscription"
  end

  Signal.trap('INT') do
    puts "INT received. Shutting down..."
    connection.close { EventMachine.stop { exit } }
  end

  consumer.start
  puts "Consumer started"
end

puts "Done"
