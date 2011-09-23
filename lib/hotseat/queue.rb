require 'time'

module Hotseat

  class QueueError < RuntimeError
  end

  class Queue
    attr_accessor :db, :config

    DEFAULT_CONFIG = {
      :design_doc_name => 'hotseat_queue',
      :pending_view_name => 'pending',
      :locked_view_name => 'locked',
      :done_view_name => 'done',
      :all_view_name => 'all',
      :object_name => 'hotseat',
    }

    def initialize(db, options={})
      @db = db
      @config = DEFAULT_CONFIG.merge(options)
      unless Hotseat.queue?(@db, @config[:design_doc_name])
        @db.save_doc design_doc
      end
    end

    def design_doc_id
      "_design/#{config[:design_doc_name]}"
    end

    def pending_view_name
      "#{config[:design_doc_name]}/#{config[:pending_view_name]}"
    end

    def locked_view_name
      "#{config[:design_doc_name]}/#{config[:locked_view_name]}"
    end

    def done_view_name
      "#{config[:design_doc_name]}/#{config[:done_view_name]}"
    end

    def all_view_name
      "#{config[:design_doc_name]}/#{config[:all_view_name]}"
    end

    def design_doc
      q = "doc.#{config[:object_name]}"
      lock = "#{q}.lock"
      done = "#{q}.done"
      pending_func = <<-JAVASCRIPT
        function(doc) { if (#{q} && !(#{lock} || #{done})) emit(#{q}.at, null); }
      JAVASCRIPT
      locked_func = <<-JAVASCRIPT
        function(doc) { if (#{q} && #{lock}) emit(#{lock}.at, null); }
      JAVASCRIPT
      done_func = <<-JAVASCRIPT
        function(doc) { if (#{q} && #{done}) emit(#{done}.at, null); }
      JAVASCRIPT
      all_func = <<-JAVASCRIPT
        function(doc) { if (#{q}) emit(#{q}.at, null); }
      JAVASCRIPT
      {
        '_id' => "_design/#{config[:design_doc_name]}",
        :views => {
          config[:pending_view_name] => { :map => pending_func.strip },
          config[:locked_view_name] => { :map => locked_func.strip },
          config[:done_view_name] => { :map => done_func.strip },
          config[:all_view_name] => { :map => all_func.strip },
        }
      }
    end

    def patch(doc)
      doc[config[:object_name]] = {'at' => Time.now.utc.iso8601, 'by' => $$}
      doc
    end

    def unpatch(doc)
      doc.delete( config[:object_name] )
      doc
    end

    def add_lock(doc)
      obj = doc[config[:object_name]]
      obj['lock'] = {'at' => Time.now.utc.iso8601, 'by' => $$}
      doc
    end

    def locked?(doc)
      if obj = doc[config[:object_name]]
        obj.has_key? 'lock'
      end
    end

    def remove_lock(doc)
      obj = doc[config[:object_name]]
      obj.delete 'lock'
      doc
    end

    def mark_done(doc)
      obj = doc[config[:object_name]]
      obj['done'] = {'at' => Time.now.utc.iso8601, 'by' => $$}
      doc
    end

    def add(doc_id)
      @db.update_doc(doc_id) do |doc|
        patch doc
        yield doc if block_given?
      end
    end

    def add_bulk(doc_ids)
      #Note: this silently ignores missing doc_ids
      docs = @db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.compact
      docs.each {|doc| patch doc }
      @db.bulk_save docs, use_uuids=false
    end

    def num_pending
      @db.view(pending_view_name, :limit => 0)['total_rows']
    end
    alias :size :num_pending

    def get(n=1)
      rows = @db.view(pending_view_name, :limit => n, :include_docs => true)['rows']
      rows.map{|row| row['doc']} unless rows.empty?
    end

    def lease(n=1)
      if docs = get(n)
        docs.each {|doc| add_lock doc }
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
      @db.view(locked_view_name, :limit => 0)['total_rows']
    end

    def unlease(doc_id)
      @db.update_doc(doc_id) do |doc|
        raise(QueueError, "Document is already unlocked") unless locked?(doc)
        remove_lock doc
        yield doc if block_given?
      end
    end

    def remove(doc_id, opts={})
      @db.update_doc(doc_id) do |doc|
        raise(QueueError, "Document was already removed") unless locked?(doc)
        if opts.delete(:forget)
          unpatch doc
        else
          mark_done( remove_lock( doc ) )
        end
        yield doc if block_given?
      end
    end

    def remove_bulk(doc_ids, opts={})
      rows = @db.bulk_load(doc_ids)['rows']
      docs, missing = rows.partition {|row| row['doc'] }
      docs.map! {|row| row['doc'] }
      locked, unlocked = docs.partition {|doc| locked? doc }
      forget = opts.delete(:forget)
      locked.each do |doc|
        if forget
          unpatch doc
        else
          mark_done( remove_lock( doc ) )
        end
      end
      @db.bulk_save locked, use_uuids=false
      {'errors' =>
        unlocked.map {|doc| {'id' => doc['_id'], 'error' => 'unlocked' } } +
        missing.map {|row| {'id' => row['key'], 'error' => row['error']} }
      }
    end

    def num_done
      @db.view(done_view_name, :limit => 0)['total_rows']
    end

    def num_all
      @db.view(all_view_name, :limit => 0)['total_rows']
    end
    alias :num_total :num_all

    def forget(doc_id)
      @db.update_doc(doc_id) do |doc|
        unpatch doc
      end
    end

    def forget_bulk(doc_ids)
      #Note: this silently ignores missing doc_ids
      docs = @db.bulk_load(doc_ids)['rows'].map{|row| row['doc']}.compact
      docs.each {|doc| unpatch doc }
      @db.bulk_save docs, use_uuids=false
    end

    def purge
      rows = @db.view(all_view_name, :include_docs => true)['rows']
      docs = rows.map{|row| row['doc']}
      docs.each{|doc| unpatch doc }
      @db.bulk_save docs, use_uuids=false
    end

  end
end
