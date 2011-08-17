require 'time'

module Hotseat

  class QueueError < RuntimeError
  end

  class Queue
    attr_reader :db

    class << self

      def patch(doc)
        doc[Hotseat.config[:object_name]] = {'at' => Time.now.utc.iso8601, 'by' => $$}
        doc
      end

      def unpatch(doc)
        doc.delete( Hotseat.config[:object_name] )
        doc
      end

      def add_lock(doc)
        obj = doc[Hotseat.config[:object_name]]
        obj['lock'] = {'at' => Time.now.utc.iso8601, 'by' => $$}
        doc
      end

      def locked?(doc)
        if obj = doc[Hotseat.config[:object_name]]
          obj.has_key? 'lock'
        end
      end

      def remove_lock(doc)
        obj = doc[Hotseat.config[:object_name]]
        obj.delete 'lock'
        doc
      end

      def mark_done(doc)
        obj = doc[Hotseat.config[:object_name]]
        obj['done'] = {'at' => Time.now.utc.iso8601, 'by' => $$}
        doc
      end

    end

    def initialize(db)
      @db = db
      unless Hotseat.queue?(@db)
        @db.save_doc Hotseat.design_doc
      end
    end

    def add(doc_id)
      @db.update_doc(doc_id) do |doc|
        Queue.patch doc
        yield doc if block_given?
      end
    end

    def add_bulk(doc_ids)
      #Note: this silently ignores missing doc_ids
      docs = @db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.compact
      docs.each {|doc| Queue.patch doc }
      @db.bulk_save docs, use_uuids=false
    end

    def num_pending
      @db.view(Hotseat.pending_view_name, :limit => 0)['total_rows']
    end
    alias :size :num_pending

    def get(n=1)
      rows = @db.view(Hotseat.pending_view_name, :limit => n, :include_docs => true)['rows']
      rows.map{|row| row['doc']} unless rows.empty?
    end

    def lease(n=1)
      if docs = get(n)
        docs.each {|doc| Queue.add_lock doc }
        response = @db.bulk_save docs, use_uuids=false
        # Some docs may have failed to lock - probably updated by another process
        locked_ids = response.reject{|res| res['error']}.map{|res| res['id']}
        if locked_ids.length < docs.length
          # This runs in O(n^2) time. Performance will be bad here if the number of documents
          # is very large. Assuming that this isn't normally the case I'm keeping it simple.
          docs.keep_if{|doc| locked_ids.include? doc['_id']}
        end
        docs
      end
    end

    def num_locked
      @db.view(Hotseat.locked_view_name, :limit => 0)['total_rows']
    end

    def remove(doc_id, opts={})
      @db.update_doc(doc_id) do |doc|
        raise(QueueError, "Document was already removed") unless Queue.locked?(doc)
        if opts.delete(:forget)
          Queue.unpatch doc
        else
          Queue.mark_done( Queue.remove_lock( doc ) )
        end
        yield doc if block_given?
      end
    end

    def remove_bulk(doc_ids, opts={})
      rows = @db.bulk_load(doc_ids)['rows']
      docs, missing = rows.partition {|row| row['doc'] }
      docs.map! {|row| row['doc'] }
      locked, unlocked = docs.partition {|doc| Queue.locked? doc }
      forget = opts.delete(:forget)
      locked.each do |doc|
        if forget
          Queue.unpatch doc
        else
          Queue.mark_done( Queue.remove_lock( doc ) )
        end
      end
      @db.bulk_save locked, use_uuids=false
      {'errors' =>
        unlocked.map {|doc| {'id' => doc['_id'], 'error' => 'unlocked' } } +
        missing.map {|row| {'id' => row['key'], 'error' => row['error']} }
      }
    end

    def num_done
      @db.view(Hotseat.done_view_name, :limit => 0)['total_rows']
    end

    def num_all
      @db.view(Hotseat.all_view_name, :limit => 0)['total_rows']
    end
    alias :num_total :num_all

    def forget(doc_id)
      @db.update_doc(doc_id) do |doc|
        Queue.unpatch doc
      end
    end

    def forget_bulk(doc_ids)
      #Note: this silently ignores missing doc_ids
      docs = @db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.compact
      docs.each {|doc| Queue.unpatch doc }
      @db.bulk_save docs, use_uuids=false
    end

    def purge
      rows = @db.view(Hotseat.all_view_name, :include_docs => true)['rows']
      docs = rows.map{|row| row['doc']}
      docs.each{|doc| Queue.unpatch doc }
      @db.bulk_save docs, use_uuids=false
    end

  end
end
