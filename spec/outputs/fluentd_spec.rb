# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/plain"
require "logstash/event"

require "logstash/outputs/treasure_data"

describe LogStash::Outputs::TreasureData do
  let(:sample_event) { LogStash::Event.new }
  let(:output) { LogStash::Outputs::TreasureData.new(config) }
  let(:config) do
    {
      "apikey" => "0/xxxxxxxxxxxxxxxxx",
      "database" => "logstash",
      "table" => "spec",
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
