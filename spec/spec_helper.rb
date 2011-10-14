$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

require 'dagqueue'

require 'rspec'
require 'fileutils'
require 'rake'

include RakeFileUtils

PATH = File.expand_path('../..', __FILE__)

def fixture_path(*path)
  File.join(PATH, 'spec', 'fixtures', *path)
end

def temp_path(*path)
  File.join(PATH, 'temp', *path)
end

REDIS_SERVER_COMMAND =
    %{ redis-server "#{fixture_path('redis.conf')}" > /dev/null 2>&1}

REDIS_CONNECT_STRING = 'redis://127.0.0.1:9736/0'

def start_redis
  stop_redis
  mkdir_p(temp_path)
  connect_string = REDIS_CONNECT_STRING
  chdir temp_path do
    begin
      `#{REDIS_SERVER_COMMAND}`
    rescue
      raise(RSpec::Expectations::ExpectationNotMetError,
            "Could not start redis-server")
    else
      $test_redis  = Redis.connect(:url => connect_string)
      Resque.redis = connect_string
    end
  end
  begin
    $test_redis.ping
  rescue
    sleep 0.2; retry
  end
end

def stop_redis
  if redis_running?
    clear_redis
    begin
      $test_redis.shutdown
    rescue Errno::ECONNREFUSED
      # This always happens when shutting down
    end
    rm_rf temp_path 'dump.rdb'
    rm_rf temp_path 'redis.pid'
  end
end

def redis_running?
  begin
    defined?($test_redis) &&
        $test_redis.inspect =~ /#{REDIS_CONNECT_STRING}/ &&
        $test_redis.ping
  rescue
    return nil
  end
end

def clear_redis
  $test_redis.flushall if redis_running?
end


RSpec.configure do |config|

  config.before( :suite ) do
    puts "starting redis test instance at #{REDIS_CONNECT_STRING}"
    start_redis unless redis_running?
  end

  config.after( :suite ) do
    stop_redis
    puts "stopped redis test instance"
  end

  config.before(:each) { clear_redis }
  
end

# So test runner doesn't display connection string
module Dagqueue
  def to_s
    Dagqueue.name
  end
end

# Open up private methods. For testing internals that we don't want
# to expose to the API client.
# http://blog.jayfields.com/2007/11/ruby-testing-private-methods.html
class Class
  def publicize_methods
    saved_private_instance_methods = self.private_instance_methods
    self.class_eval { public *saved_private_instance_methods }
    yield
    self.class_eval { private *saved_private_instance_methods }
  end
end

