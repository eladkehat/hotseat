require File.expand_path("../../spec_helper", __FILE__)

module Hotseat
  describe Service do

    describe "#make_queue" do
      it "should make a queue out of a given database"

      it "should create the database if it doesn't exist"
    end

    describe "#queue?" do
      it "should be true if a given database is a queue"

      it "should be false if a given database is not a queue"

      it "should be false if a given database does not exist"
    end

    describe "#queues" do
      it "should return a list of all queues on the host"

    end
  end
end
