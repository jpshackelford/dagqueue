require File.expand_path('../../lib/dagqueue', __FILE__)

# Add jobs to a queue
class MyJob < Dagqueue::Task
end

# Create the dependency graph of jobs
jobs = Array.new(4){ MyJob.new }
jobs[1].requires( jobs[0] ) # Dagqueue can handle thousands of
jobs[2].requires( jobs[0] ) # complex dependency relationships in
jobs[1].requires( jobs[2] ) # a single DAG.

dag = Dagqueue::Dag.new( jobs, 'some_queue_name' )

# Enqueue the graph
Dagqueue.enqueue( dag )

# Enqueue a few more graphs and watch the magic!
