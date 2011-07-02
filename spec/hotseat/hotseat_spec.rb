require File.expand_path("../../spec_helper", __FILE__)

module Hotseat

  describe "#queue" do
    it "should return a queue on the given database" do
      reset_test_db!
      q = Hotseat.queue DB
      q.should be_instance_of Hotseat::Queue
      Hotseat.queue?(DB).should be_true
    end

  end

  describe "#queue?" do
    it "should be true when a given database is a queue" do
      reset_test_db!
      Hotseat.queue DB
      Hotseat.queue?(DB).should be_true
    end

    it "should be false when a given database is not a queue" do
      reset_test_db!
      Hotseat.queue?(DB).should be_false
    end

    it "should be false when a given database does not exist" do
      delete_test_db!
      Hotseat.queue?(DB).should be_false
    end
  end

  describe "#queues" do
    # not making sure that non-queues aren't returned since the host we test on
    # may be have some non-testing (production) queues on it which we don't know about
    it "should return a list of queues (database names) on the host" do
      delete_test_db!
      test_dbs = (1..3).map{|i| TEST_SERVER.database("#{TESTDB}#{i}") }
      begin
        test_dbs.each do |db|
          db.create!
          Hotseat.queue(db)
        end
        results = Hotseat.queues TEST_SERVER
        test_dbs.each {|db| results.should include URI.unescape(db.name) }
      ensure test_dbs.each{|db| db.delete! rescue nil } end
    end
  end

end
