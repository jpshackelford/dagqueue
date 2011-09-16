require File.expand_path('../../lib/dagqueue', __FILE__)

# Define the work to be done
class MyJob < Dagqueue::Task
  def self.perform(*args)
    sleep 3 # heavy lifting
    puts "Finished #{self} #{args}"
  end
end


# Start the worker Worker
worker              = Dagqueue::Worker.new('some_queue_name::dags')
worker.very_verbose = true
worker.work 5
