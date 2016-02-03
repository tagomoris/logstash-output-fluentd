# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"

require "stud/buffer"
require "stud/try"
require "socket"

require "msgpack"

require_relative "./fluentd/version"

class LogStash::Event
  def to_msgpack(packer=nil)
    # LogStash objects (ex: LogStash::Timestamp) are impossible to serialize by msgpack
    begin
      @data.reject{|a,b| a == TIMESTAMP }.to_msgpack
    rescue ArgumentError => e
      LogStash::Json.load(@data.to_json).to_msgpack
    end
  end
end

class LogStash::Outputs::Fluentd < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "fluentd"

  config :host, validate: :string, required: true
  config :port, validate: :number, default: 24224
  # config :policy, validate: :string, default: 'at-most-once' # TODO: at-least-once

  config :tag, validate: :string, default: 'logstash'
  # config :use_ssl, validate: :boolean, default: false # TODO: true

  # config :connect_timeout, validate: :number, default: nil
  # config :read_timeout, validate: :number, default: nil
  # config :send_timeout, validate: :number, default: nil

  config :flush_size, validate: :number, default: 50    # 50 records
  config :flush_interval, validate: :number, default: 5 # 5 seconds

  public
  def register
    buffer_initialize(
      max_items: @flush_size,
      max_interval: @flush_interval,
      logger: @logger
    )
  end # def register

  public
  def receive(event)
    @logger.debug "receive a event"

    entry = [(event.timestamp.to_i || Time.now.to_i), event]
    buffer_receive(entry.to_msgpack)

    @logger.debug "buffered a event"
  end

  public
  def flush(events, teardown = false)
    @logger.debug "flushing #{events} events"
    return if events.size < 1

    data = [@tag, events.join].to_msgpack

    @logger.debug "sending chunk #{data.bytesize} bytes"
    connect.write data

    @logger.debug "done"
  end # def flush

  public
  def on_flush_error(e)
    @logger.warn "flush error #{e.class}: #{e.message}"
  end

  public
  def on_full_buffer_error(opts={})
    @logger.warn "buffer exceeds limits: pending:#{opts[:pending_count]}, outgoing:#{opts[:outgoing_count]}"
  end

  public
  def close
    buffer_flush(final: true)
  end

  private
  def connect
    Stud::try do
      return TCPSocket.new(@host, @port)
    end
  end # def connect
end # class LogStash::Outputs::Example
