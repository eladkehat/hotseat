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
        q.db.get(q.design_doc_id).should_not be_nil
      end
    end

    describe "#patch" do
      before(:each) do
        reset_test_queue!
      end

      it "should add a queue object on a document" do
        doc = sample_doc
        @q.patch doc
        doc.should have_key(@q.config[:object_name])
      end

      it "should return the patched document with original data intact" do
        doc = @q.patch sample_doc
        sample_doc.each do |k,v|
          doc[k].should == v
        end
      end
    end

    describe "#unpatch" do
      it "should remove the queue object from a document" do
        reset_test_queue!
        doc = @q.unpatch( @q.patch( sample_doc ) )
        doc.should_not have_key(@q.config[:object_name])
      end
    end

    describe "#add_lock" do
      it "should add a lock object on a patched document" do
        reset_test_queue!
        doc = @q.add_lock( @q.patch( sample_doc ) )
        patch = doc[@q.config[:object_name]]
        patch.should have_key('lock')
      end
    end

    describe "#remove_lock" do
      before(:each) do
        reset_test_queue!
        @doc = @q.remove_lock( @q.add_lock( @q.patch( sample_doc ) ) ) 
      end
      it "should remove a lock object added by #add_lock" do
        patch = @doc[@q.config[:object_name]]
        patch.should_not have_key('lock')
      end
      it "should leave the queue patch intact" do
        @doc.should have_key(@q.config[:object_name])
      end
    end

    describe "#locked?" do
      before(:each) do
        reset_test_queue!
      end
      it "should be true for a locked document" do
        doc = @q.add_lock( @q.patch( sample_doc ) )
        @q.locked?(doc).should be_true
      end
      it "should be false for a patched, but not locked document" do
        doc = @q.patch( sample_doc )
        @q.locked?(doc).should be_false
      end
      it "should be false for an unlocked document" do
        doc = @q.remove_lock( @q.add_lock( @q.patch( sample_doc ) ) )
        @q.locked?(doc).should be_false
      end
    end

    describe "#mark_done" do
      before(:each) do
        reset_test_queue!
        @doc = @q.mark_done( @q.patch( sample_doc ) )
      end
      it "should leave the queue patch intact" do
        @doc.should have_key(@q.config[:object_name])
      end
      it "should add a 'done' object on a patched document" do
        patch = @doc[@q.config[:object_name]]
        patch.should have_key('done')
      end
    end

    describe "#add" do
      before(:each) do
        reset_test_queue!
        @doc_id = DB.save_doc(sample_doc)['id']
      end

      it "should add a document to the queue, given a doc id" do
        @q.add @doc_id
        DB.get(@doc_id).should have_key(@q.config[:object_name])
      end

      it "should save changes made in the block" do
        @q.add(@doc_id) do |doc|
          doc['field'] = 'changed value'
          doc['another_field'] = 'another value'
        end
        doc = DB.get(@doc_id)
        doc['field'].should == 'changed value'
        doc['another_field'].should == 'another value'
      end
    end

    describe "#add_bulk" do
      it "should add multiple documents in bulk, given multiple doc ids" do
        reset_test_queue!
        doc_ids = create_some_docs
        @q.add_bulk doc_ids
        doc_ids.each do |doc_id|
          DB.get(doc_id).should have_key(@q.config[:object_name])
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
          db_doc.should have_key(@q.config[:object_name])
        end
      end

      it "should lock a pending document" do
        doc_id = @q.lease.first['_id']
        doc = DB.get(doc_id)
        doc.should have_key(@q.config[:object_name])
        doc[@q.config[:object_name]].should have_key('lock')
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
        pending_ids = @q.db.view(@q.pending_view_name)['rows'].map{|row| row['id']}
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
          db_doc.should have_key(@q.config[:object_name])
        end
      end

      it "should not lock the documents it returns" do
        doc_id = @q.get.first['_id']
        doc = DB.get(doc_id)
        doc.should have_key(@q.config[:object_name])
        doc[@q.config[:object_name]].should_not have_key('lock')
      end

      it "should return up to the specified number of documents" do
        docs = @q.get 4
        docs.should have(3).items
      end
    end

    describe "#unlease" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(3) )
        @leased = @q.lease 2
        @doc_id = @leased.first['_id']
      end

      it "should unlock a leased document" do
        @q.unlease @doc_id
        doc = DB.get(@doc_id)
        doc.should have_key(@q.config[:object_name])
        doc[@q.config[:object_name]].should_not have_key('lock')
      end

      it "should leave the document in the queue" do
        @q.unlease @doc_id
        pending_docs = @q.get 3 # ensure we get all remaining pending docs
        pending_docs.map{|doc| doc['_id']}.should include(@doc_id)
      end

      it "should raise an error if the lock was already removed" do
        doc = @leased.first
        @q.unlease @doc_id
        expect {
          @q.unlease @doc_id
        }.to raise_error(Hotseat::QueueError)
      end

      it "should raise an error if the document is missing from the database" do
        doc_id = @leased.first['_id']
        doc = @q.db.get(doc_id)
        @q.db.delete_doc(doc)
        expect {
          @q.unlease doc['_id']
        }.to raise_error
      end

      it "should save any changes made in the block" do
        @q.unlease(@doc_id) do |doc|
          doc['field'] = 'changed value'
          doc['another_field'] = 'another value'
        end
        doc = DB.get(@doc_id)
        doc['field'].should == 'changed value'
        doc['another_field'].should == 'another value'
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
        doc.should have_key(@q.config[:object_name])
        doc[@q.config[:object_name]].should_not have_key('lock')
      end

      it "should remove a document from the queue" do
        @q.remove @doc_id
        pending_docs = @q.get 3 # ensure we get all remaining pending docs
        pending_docs.map{|doc| doc['_id']}.should_not include(@doc_id)
      end

      it "should raise an error if the lock was already removed" do
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
        doc.should have_key(@q.config[:object_name])
        doc[@q.config[:object_name]].should have_key('done')
      end

      it "should delete queue history from the document when forget=true" do
        @q.remove @doc_id, :forget => true
        doc = DB.get(@doc_id)
        doc.should_not have_key(@q.config[:object_name])
      end

      it "should save any changes made in the block" do
        @q.remove(@doc_id) do |doc|
          doc['field'] = 'changed value'
          doc['another_field'] = 'another value'
        end
        doc = DB.get(@doc_id)
        doc['field'].should == 'changed value'
        doc['another_field'].should == 'another value'
      end
    end

    describe "#remove_bulk" do
      before(:each) do
        reset_test_queue!
        enqueue( create_some_docs(10) )
        @leased = @q.lease 8
        @doc_ids = @leased.take(5).map{|doc| doc['_id'] }
      end

      it "should unlock leased documents" do
        @q.remove_bulk @doc_ids
        docs = DB.get_bulk(@doc_ids)['rows'].map{|row| row['doc']}
        docs.each do |doc|
          doc.should have_key(@q.config[:object_name])
          doc[@q.config[:object_name]].should_not have_key('lock')
        end
      end

      it "should remove multiple documents from the queue" do
        @q.remove_bulk @doc_ids
        pending_docs = @q.get 3 # ensure we get all remaining pending docs
        pending_ids = pending_docs.map{|doc| doc['_id'] }
        (pending_ids - @doc_ids).should eql(pending_ids)
      end

      it "should report docs whose lock was already removed" do
        rem_ids = @doc_ids.take(2)
        @q.remove_bulk rem_ids
        res = @q.remove_bulk @doc_ids
        res['errors'].should have(2).errors
        res['errors'].map{|err| err['id']}.should == rem_ids
      end

      it "should report docs that are missing from the database" do
        rem_ids = @doc_ids.take(2)
        docs = rem_ids.map{|id| @q.db.get(id) }
        docs.each {|doc| @q.db.delete_doc(doc) }
        res = @q.remove_bulk @doc_ids
        res['errors'].should have(2).errors
        res['errors'].map{|err| err['id']}.should == rem_ids
      end

      it "should leave queue history in the document (mark as done) by default" do
        @q.remove_bulk @doc_ids
        docs = DB.get_bulk(@doc_ids)['rows'].map{|row| row['doc']}
        docs.each do |doc|
          doc.should have_key(@q.config[:object_name])
          doc[@q.config[:object_name]].should have_key('done')
        end
      end

      it "should delete queue history from the document when forget=true" do
        @q.remove_bulk @doc_ids, :forget => true
        docs = DB.get_bulk(@doc_ids)['rows'].map{|row| row['doc']}
        docs.each do |doc|
          doc.should_not have_key(@q.config[:object_name])
        end
      end
    end

    describe "#num_done" do
      it "should return the number of documents done" do
        reset_test_queue!
        enqueue( create_some_docs(10) )
        @leased = @q.lease 8
        @doc_ids = @leased.take(5).map{|doc| doc['_id'] }
        @q.remove_bulk @doc_ids
        @q.num_done.should == 5
      end
    end

    describe "#num_all" do
      it "should return the total number of documents in the queue" do
        reset_test_queue!
        enqueue( create_some_docs(10) )
        @leased = @q.lease 8
        @doc_ids = @leased.take(5).map{|doc| doc['_id'] }
        @q.remove_bulk @doc_ids
        @q.num_all.should == 10
      end
    end

    describe "#forget" do
      it "should delete queue history from a document" do
        reset_test_queue!
        enqueue( create_some_docs(1) )
        doc_id = @q.get.first['_id']
        @q.forget doc_id
        doc = DB.get(doc_id)
        doc.should_not have_key(@q.config[:object_name])
      end
    end

    describe "#forget_bulk" do
      it "should delete queue history from multiple document" do
        reset_test_queue!
        enqueue( create_some_docs(3) )
        doc_ids = @q.get(3).map{|doc| doc['_id'] }
        @q.forget_bulk doc_ids
        @q.db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.each do |doc|
          doc.should_not have_key(@q.config[:object_name])
        end
      end
    end

    describe "#purge" do
      before do
        reset_test_queue!
        enqueue( create_some_docs(10) )
        leased = @q.lease 5
        @q.remove_bulk leased.take(2)
      end

      it "should remove (and forget) all documents from the queue" do
        @q.purge
        @q.num_all.should == 0
      end
    end

    context "when two queues are defined on the same DB" do
      before do
        reset_test_db!
        @doc_ids = create_some_docs(10)
        @q1 = Hotseat.make_queue(DB, :design_doc_name => 'hotseat1_queue', :object_name => 'hotseat1')
        @q2 = Hotseat.make_queue(DB, :design_doc_name => 'hotseat2_queue', :object_name => 'hotseat2')
      end

      it "#add should add a doc to the specified queue only" do
        @q1.add @doc_ids[0]
        @q2.add @doc_ids[1]
        DB.get(@doc_ids[0]).should have_key(@q1.config[:object_name])
        DB.get(@doc_ids[0]).should_not have_key(@q2.config[:object_name])
        DB.get(@doc_ids[1]).should have_key(@q2.config[:object_name])
        DB.get(@doc_ids[1]).should_not have_key(@q1.config[:object_name])
      end

      it "#lease should lease docs from the specified queue" do
        @q1.add_bulk @doc_ids[0, 5]
        @q2.add_bulk @doc_ids[5, 5]
        doc1 = @q1.lease.first
        @doc_ids[0, 5].should include(doc1['_id'])
        @q2.lease(3).each do |doc|
          @doc_ids[5, 5].should include(doc['_id'])
        end
      end
    end
  end
end
