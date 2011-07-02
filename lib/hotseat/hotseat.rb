module Hotseat

  class << self

    def queue(db)
      Queue.new(db)
    end
    alias :make_queue :queue

    def queue?(db, config=CONFIG)
      # ignore system dbs like _replicator and _users
      return false if db.name =~ /^_/
      begin
        db.get design_doc_id(config)
      rescue RestClient::ResourceNotFound
      # either the database or the design doc does not exist
        false
      end
    end

    def queues(couch_server, config=CONFIG)
      couch_server.databases.select do |db|
        queue?(couch_server.database(db), config)
      end
    end

    def design_doc_id(config=CONFIG)
      "_design/#{config[:design_doc_name]}"
    end

    def design_doc(config=CONFIG)
      {
        '_id' => "_design/#{config[:design_doc_name]}",
        :views => {
          config[:pending_view_name] => {
            :map => "function(doc) { if (!doc.lock) emit(doc.set_at, null);}"
          },
          config[:locked_view_name] => {
            :map => "function(doc) { if (doc.lock) emit(doc.lock.locked_at, null);}"
          }
        }
      }
    end

  end

  CONFIG = {
    :design_doc_name => 'hotseat_queue',
    :pending_view_name => 'pending',
    :locked_view_name => 'locked',
    :default_visibility_timeout => 30_000,
  }

end