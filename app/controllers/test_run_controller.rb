require 'amqp/utilities/event_loop_helper'
require 'bunny'

class TestRunController < ApplicationController

  #t = Thread.new { EventMachine.run }

  AMQP_OPTS = {:host => '127.0.0.1'}

  def run
    AMQP::Utilities::EventLoopHelper.run do

      connection = AMQP.connect(AMQP_OPTS)

      channel               = AMQP::Channel.new(connection)
      channel.auto_recovery = true

      channel.queue("rails.helloworld", :durable => true, :persistent => true)
      exchange = channel.default_exchange

      #exchange.on_connection_interruption do |ex|
      #  logger.error "Exchange #{ex.name} detected connection interruption"
      #end

      exchange.publish("Hello, world!", :routing_key => "rails.helloworld", :mandatory => true, :persistent => true) do
        logger.info "foo"
      end

    end

    render :text => 'Foo'
  end

  def bunny
    #push messages to queue
    Bunny.run(AMQP_CONFIG) do |b|
      b.queue("queue.#{queue}", :durable => true).publish(message.to_json, :mandatory => true, :persistent => true)

    end
  end

  def read
    messages = []

    AMQP.start(AMQP_OPTS) do |connection|
      channel = AMQP::Channel.new(connection)
      queue   = channel.queue("rails.helloworld", :durable => true)

      logger.info "Got queue for read"

      queue.status do |number_of_messages, number_of_consumers|
        logger.info "Found #{number_of_messages} messages with #{number_of_consumers} consumers"
        messages << "Number of messages on queue = #{number_of_messages}"
        number_of_messages.times do
          queue.pop do |payload|
            logger.info "Got message #{payload}"
            messages << payload if payload
          end
        end
      end

      show_stopper = Proc.new do
        logger.info "Stopping..."
        connection.close { EventMachine.stop }
      end

      sleep 2
      EM.add_timer(2, show_stopper)
    end

    logger.info "Done, rendering messages"
    render :text => messages.join('<br/>')
  end

end
