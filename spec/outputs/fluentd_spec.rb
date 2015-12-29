# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/plain"
require "logstash/event"

require "logstash/outputs/fluentd"

describe LogStash::Outputs::Fluentd do
  let(:sample_event) { LogStash::Event.new }
  let(:output) { LogStash::Outputs::Fluentd.new(config) }
  let(:config) do
    {
      "host" => "127.0.0.1"
    }
  end

  before do
    output.register
  end

  describe "receive message" do
    subject { output.receive(sample_event) }

    it "returns a nil" do
      expect(subject).to be_nil
    end
  end
end
