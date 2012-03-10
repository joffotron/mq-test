#!/usr/bin/env ruby
# encoding: utf-8

require 'amqp'

# Would normally do this via Round-robin DNS or a Load-balancer
AMQP_NODE_1 = {:host => '127.0.0.1', :port => 5672}
AMQP_NODE_2 = {:host => '127.0.0.1', :port => 5673}

class TestProducer
  def initialize(exchange)
    @exchange = exchange
  end

  def say_hello
    @exchange.publish("Hello, world!", :routing_key => "rails.helloworld", :mandatory => true, :persistent => true) do
      puts "Said Hello"
    end
  end

end

def next_server
  (@current_server == AMQP_NODE_1) ? AMQP_NODE_2 : AMQP_NODE_1
end

EventMachine.run do
  @current_server = AMQP_NODE_1

  connection = AMQP.connect(@current_server)

  connection.on_tcp_connection_loss do
    puts "Lost connection to #{@current_server[:host]}:#{@current_server[:port]}"
    @current_server = next_server
    connection.reconnect_to(@current_server)
  end

  connection.on_recovery do
    puts "Re-connected to #{@current_server[:host]}:#{@current_server[:port]}"
  end

  channel               = AMQP::Channel.new(connection)
  channel.auto_recovery = true

  channel.queue("rails.helloworld", :durable => true,:'x-ha-policy' => 'all')
  test_producer = TestProducer.new(channel.default_exchange)

  EventMachine.add_periodic_timer(2.0) do
    test_producer.say_hello
  end

  Signal.trap('INT') do
    puts "INT received. Shutting down..."
    connection.close{EventMachine.stop{exit}}
  end

end