module Hotseat

  class << self

    def queue(db)
      Hotseat::Queue.new(db)
    end
    alias :make_queue :queue

    def queue?(db)
      # ignore system dbs like _replicator and _users
      return false if db.name =~ /^_/
      begin
        db.get design_doc_id
      rescue RestClient::ResourceNotFound
      # either the database or the design doc does not exist
        false
      end
    end

    def queues(couch_server)
      couch_server.databases.select do |db|
        queue?(couch_server.database(db))
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
          config[:pending_view_name] => { :map => pending_func.chomp },
          config[:locked_view_name] => { :map => locked_func.chomp },
          config[:done_view_name] => { :map => done_func.chomp },
          config[:all_view_name] => { :map => all_func.chomp },
        }
      }
    end

    def config
      CONFIG
    end

  end

  CONFIG = {
    :design_doc_name => 'hotseat_queue',
    :pending_view_name => 'pending',
    :locked_view_name => 'locked',
    :done_view_name => 'done',
    :all_view_name => 'all',
    :object_name => 'hotseat',
  }

end