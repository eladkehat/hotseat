module Hotseat
  class Queue
    attr_reader :db

    def initialize(db)
      @db = db
      unless Hotseat.queue?(@db)
        @db.save_doc Hotseat.design_doc
      end
    end

  end
end