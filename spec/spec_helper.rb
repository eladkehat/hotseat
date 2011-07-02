require 'rubygems'
require 'bundler'
Bundler.require(:default, :test)

$LOAD_PATH << File.expand_path('../../lib', __FILE__)
require 'hotseat'

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
#Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

# copied and modified from https://github.com/couchrest/couchrest/blob/master/spec/spec_helper.rb
unless defined?(TESTDB)
  COUCHHOST = ENV['COUCHHOST'] || "http://127.0.0.1:5984"
  TESTDB = "hotseat%2Ftest"
  TEST_SERVER = CouchRest.new COUCHHOST
  TEST_SERVER.default_database = TESTDB
  DB = TEST_SERVER.database(TESTDB)
end

def reset_test_db!
  DB.recreate! rescue nil
  DB
end

def delete_test_db!
  DB.delete! rescue nil
end

def clean_up_test_dbs
  cr = TEST_SERVER
  test_dbs = cr.databases.select { |db| db =~ /^#{URI.unescape(TESTDB)}/ }
  test_dbs.each do |db|
    cr.database(db).delete! rescue nil
  end
end

RSpec.configure do |config|
  config.after(:all) do
    clean_up_test_dbs
  end
end