# std libraries
require 'set'

# rubygems
require 'resque'
require 'redis/objects'
require 'rgl/adjacency'

# If we fail to load our own files, tinker with the $LOAD_PATH
# otherwise, don't mess with it'
begin

  # project files
  require 'dagqueue/redis_functions'
  require 'dagqueue/dag'
  require 'dagqueue/dag_queue'
  require 'dagqueue/task'
  require 'dagqueue/worker'

  # augment Resque
  # NOT A MONKEY PATCH!
  require 'resque/job_queue'

  # patches
  require 'dagqueue/core/float'

rescue LoadError => e
  paths     = $LOAD_PATH.map { |p| File.expand_path(p) }
  this_path = File.expand_path('..', __FILE__)
  unless paths.include?(this_path)
    $LOAD_PATH.unshift(this_path)
  else
    raise e
  end
  retry
end


module Dagqueue

  extend Resque
  extend self

  class RecordNotFound < RuntimeError;
  end

  def enqueue(object)
    if object.is_a?(Dag)
      DagQueue.enqueue(object.queue, object)
    else
      Resque.enqueue(object)
    end
  end


end
