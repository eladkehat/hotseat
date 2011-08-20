module Hotseat

  class << self

    def queue(db, options={})
      Hotseat::Queue.new(db, options)
    end
    alias :make_queue :queue

    def queue?(db, design_doc_name = Hotseat::Queue::DEFAULT_CONFIG[:design_doc_name])
      # ignore system dbs like _replicator and _users
      return false if db.name =~ /^_/
      begin
        db.get "_design/#{design_doc_name}"
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

  end

end
