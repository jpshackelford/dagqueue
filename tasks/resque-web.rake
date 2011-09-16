
desc "start resque-web pointing to test instance of redis"
task 'web:start:test' do
  require 'vegas'
  require 'resque/server'
  require './spec/spec_helper'

  start_redis
  
  Resque.redis = Redis.new(:host => '127.0.0.1', :port => '9736')
  Vegas::Runner.new(Resque::Server, 'resque-web', {}, %w{-F --app-dir ./temp})
end
