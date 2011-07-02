require File.expand_path("../../spec_helper", __FILE__)

module Hotseat
  describe Queue do

    describe "#initialize" do
      it "should create a Hotseat design doc in the database if one does not exist" do
        DB.recreate! rescue nil
        q = Queue.new(DB)
        q.db.get(Hotseat.design_doc_id).should_not be_nil
      end
    end

    describe "#add" do
      it "should add a document to the queue"

      it "should add multiple documents in bulk"
    end

    describe "#lease" do
      it "should lock a pending document"

      it "should return a document"

      it "should lock up to the specified number of documents"

      it "should return up to the specified number of documents"

      it "should lock a document for a limited time"

      it "should return nil if no documents are pending"

    end

    context "locked documents" do
      it "should not be pending"

      it "should become pending after the lock expires"
    end

    describe "#get" do
      it "should return a pending document"

      it "should return up to the specified number of documents"

      it "should not lock documents"

      it "should return nil if no documents are pending"
    end

    describe "#remove" do
      it "should remove a document from the queue"

      it "should remove multiple documents in bulk"

      it "should raise an error if that document is locked by someone else"

      it "should fail silently if that document is missing from the database"

      it "should leave queue history in the document when forget=false"

      it "should delete queue history from the document when forget=true"
    end

    describe "#forget" do
      it "should delete queue history from document(s)"
    end

    describe "#purge" do
      it "should remove all documents from the queue"

    end
  end
end
