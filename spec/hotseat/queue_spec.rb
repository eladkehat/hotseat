require File.expand_path("../../spec_helper", __FILE__)

module Hotseat
  describe Queue do

    ########################
    ###  Helper methods  ###
    ########################
    def reset_test_queue!
      reset_test_db!
      @q = Hotseat.make_queue DB
    end

    def sample_doc(var=nil)
      {:field => var || 'value' }
    end

    # Returns the new doc id's
    def create_some_docs(n=3)
      (1..n).map {|i| DB.save_doc(sample_doc(i))['id'] }
    end

    def enqueue(doc_ids)
      @q.add_bulk doc_ids
    end

    describe "#initialize" do
      it "should create a Hotseat design doc in the database if one does not exist" do
        reset_test_db!
        q = Hotseat::Queue.new(DB)
        q.db.get(Hotseat.design_doc_id).should_not be_nil
      end
    end

    describe "#patch" do
      it "should add a queue object on a document" do
        doc = sample_doc
        Queue.patch doc
        doc.should have_key(Hotseat.config[:object_name])
      end

      it "should return the patched document with original data intact" do
        doc = Queue.patch sample_doc
        sample_doc.each do |k,v|
          doc[k].should == v
        end
      end
    end

    describe "#unpatch" do
      it "should remove the queue object from a document" do
        doc = Queue.unpatch( Queue.patch( sample_doc ) )
        doc.should_not have_key(Hotseat.config[:object_name])
      end
    end
    
    describe "#add_lock" do
      it "should add a lock object on a patched document" do
        doc = Queue.add_lock( Queue.patch( sample_doc ) )
        patch = doc[Hotseat.config[:object_name]]
        patch.should have_key('lock')
      end
    end

    describe "#remove_lock" do
      before(:each) { @doc = Queue.remove_lock( Queue.add_lock( Queue.patch( sample_doc ) ) ) }
      it "should remove a lock object added by #add_lock" do
        patch = @doc[Hotseat.config[:object_name]]
        patch.should_not have_key('lock')
      end
      it "should leave the queue patch intact" do
        @doc.should have_key(Hotseat.config[:object_name])
      end
    end
    
    describe "#locked?" do
      it "should be true for a locked document" do
        doc = Queue.add_lock( Queue.patch( sample_doc ) )
        Queue.locked?(doc).should be_true
      end
      it "should be false for a patched, but not locked document" do
        doc = Queue.patch( sample_doc )
        Queue.locked?(doc).should be_false
      end
      it "should be false for an unlocked document" do
        doc = Queue.remove_lock( Queue.add_lock( Queue.patch( sample_doc ) ) )
        Queue.locked?(doc).should be_false
      end
    end
    
    describe "#mark_done" do
      before(:each) { @doc = Queue.mark_done( Queue.patch( sample_doc ) ) }
      it "should leave the queue patch intact" do
        @doc.should have_key(Hotseat.config[:object_name])
      end
      it "should add a 'done' object on a patched document" do
        patch = @doc[Hotseat.config[:object_name]]
        patch.should have_key('done')
      end
    end
    
    describe "#add" do
      it "should add a document to the queue, given a doc id" do
        reset_test_queue!
        doc_id = DB.save_doc(sample_doc)['id']
        @q.add doc_id
        DB.get(doc_id).should have_key(Hotseat.config[:object_name])
      end
    end

    describe "#add_bulk" do
      it "should add multiple documents in bulk, given multiple doc ids" do
        reset_test_queue!
        doc_ids = create_some_docs
        @q.add_bulk doc_ids
        doc_ids.each do |doc_id|
          DB.get(doc_id).should have_key(Hotseat.config[:object_name])
        end
      end
    end

    describe "#num_pending" do
      it "should return the number of documents available for lease" do
        reset_test_queue!
        enqueue( create_some_docs(3) )
        @q.num_pending.should == 3
      end
    end

    describe "#lease" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(3) )
      end

      it "should return an array of queued documents" do
        docs = @q.lease 2
        docs.should_not be_nil
        docs.should have(2).items
        docs.each do |doc|
          db_doc = DB.get(doc['_id'])
          db_doc.should be_kind_of CouchRest::Document
          db_doc.should have_key(Hotseat.config[:object_name])
        end
      end

      it "should lock a pending document" do
        doc_id = @q.lease.first['_id']
        doc = DB.get(doc_id)
        doc.should have_key(Hotseat.config[:object_name])
        doc[Hotseat.config[:object_name]].should have_key('lock')
      end

      it "should lock and return up to the specified number of documents" do
        ids = @q.lease 4
        ids.should have(3).items
      end
    end

    context "when no documents are pending" do
      before(:each) { reset_test_queue! }

      it "#lease should return nil" do
        @q.lease.should be_nil
      end

      it "#get should return nil" do
        @q.get.should be_nil
      end

      it "#num_pending should return zero" do
        @q.num_pending.should be_zero
      end
    end

    context "locked documents" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(3) )
      end

      it "should not be pending" do
        locked_id = @q.lease.first['_id']
        pending_ids = @q.db.view(Hotseat.pending_view_name)['rows'].map{|row| row['id']}
        pending_ids.should_not include(locked_id)
      end

      it "should be counted by #num_locked" do
        @q.lease 2
        @q.num_locked.should == 2
      end
    end

    describe "#get" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(3) )
      end

      it "should return an array of pending documents" do
        docs = @q.get 2
        docs.should_not be_nil
        docs.should have(2).items
        docs.each do |doc|
          db_doc = DB.get(doc['_id'])
          db_doc.should have_key(Hotseat.config[:object_name])
        end
      end

      it "should not lock the documents it returns" do
        doc_id = @q.get.first['_id']
        doc = DB.get(doc_id)
        doc.should have_key(Hotseat.config[:object_name])
        doc[Hotseat.config[:object_name]].should_not have_key('lock')
      end

      it "should return up to the specified number of documents" do
        docs = @q.get 4
        docs.should have(3).items
      end
    end

    describe "#remove" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(3) )
        @leased = @q.lease 2
        @doc_id = @leased.first['_id']
      end

      it "should unlock a leased document" do
        @q.remove @doc_id
        doc = DB.get(@doc_id)
        doc.should have_key(Hotseat.config[:object_name])
        doc[Hotseat.config[:object_name]].should_not have_key('lock')
      end

      it "should remove a document from the queue" do
        @q.remove @doc_id
        pending_docs = @q.get 3 # ensure we get all remaining pending docs
        pending_docs.map{|doc| doc['_id']}.should_not include(@doc_id)
      end

      it "should raise an error if the lock was removed already" do
        doc = @leased.first
        @q.remove @doc_id
        expect {
          @q.remove @doc_id
        }.to raise_error(Hotseat::QueueError)
      end

      it "should raise an error if the document is missing from the database" do
        doc_id = @leased.first['_id']
        doc = @q.db.get(doc_id)
        @q.db.delete_doc(doc)
        expect {
          @q.remove doc['_id']
        }.to raise_error
      end

      it "should leave queue history in the document (mark as done) by default" do
        @q.remove @doc_id
        doc = DB.get(@doc_id)
        doc.should have_key(Hotseat.config[:object_name])
        doc[Hotseat.config[:object_name]].should have_key('done')
      end

      it "should delete queue history from the document when forget=true" do
        @q.remove @doc_id, :forget => true
        doc = DB.get(@doc_id)
        doc.should_not have_key(Hotseat.config[:object_name])
      end
    end

    describe "#remove_bulk" do
      it "should remove multiple documents"


    end

    describe "#forget" do
      it "should delete queue history from a document" do
        reset_test_queue!
        enqueue( create_some_docs(1) )
        doc_id = @q.get.first['_id']
        @q.forget doc_id
        doc = DB.get(doc_id)
        doc.should_not have_key(Hotseat.config[:object_name])
      end
    end

    describe "#forget_bulk" do
      it "should delete queue history from multiple document" do
        reset_test_queue!
        enqueue( create_some_docs(3) )
        doc_ids = @q.get(3).map{|doc| doc['_id'] }
        @q.forget_bulk doc_ids
        @q.db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.each do |doc|
          doc.should_not have_key(Hotseat.config[:object_name])
        end
      end
    end

    describe "#purge" do
      it "should remove all documents from the queue"

    end
  end
end
