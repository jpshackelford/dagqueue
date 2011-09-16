module Dagqueue

  # A Dagqueue::Task represents a unit of work associated with a Dagqueue::Dag.
  # Dagqueue::Jobs differ from their Resque counter part in that  a Dagqueue job is
  # not directly enqueued by a client. Instead jobs are added to a Dag. When the Dag
  # is enqueued its jobs are executed in an order which respects job dependencies.
  #  Like a Resque::Job, the Dagqueue::Job is associated with a payload object. The
  # payload is a hash with two attributes: `class` and `args`. The `class` is the
  # name of the Ruby class which should be used to run the job. The `args` are an
  # array of arguments which should be passed to the Ruby class's `perform`
  # class-level method.
  #
  # You can manually run a job using this code:
  #
  #   job = Resque::Job.reserve(:high)
  #   klass = Resque::Job.constantize(job.payload['class'])
  #   klass.perform(*job.payload['args'])

  class Task # < Resque::Job
    include Resque::Helpers
    extend Resque::Helpers

    CounterKey = 'dagq:job:counter'
    JobKey     = 'dagq:job'

    class << self

      def find(unique_id)
        raise(ArgumentError, "unique_id must not be nil") if unique_id.nil?
        json = Resque.redis.get job_key(unique_id)
        job  = new(decode(json)) #  nope we need to create object of appropriate type
        job.send(:instance_variable_set, :@unique_id, unique_id)
        return job
      end

      def job_key(unique_id)
        "#{JobKey}:#{unique_id}"
      end

    end

    # The worker object which is currently processing this task.
    attr_accessor :worker

    # This tasks's associated payload object.
    attr_reader :payload

    # The name of the queue from which this task was pulled (or is to be
    # placed)
    attr_reader :queue

    def initialize(payload = { }, dag = nil)
      raise ArgumentError,"payload must be a hash" unless payload.is_a?( Hash )

      @redis        = Resque.redis
      @dag          = dag
      @payload      = payload
      @dependencies = Set.new
    end

    def unique_id
      unless @unique_id
        @redis.setnx(CounterKey, 0)
        @unique_id = @redis.incrby(CounterKey, 1).to_s
      end
      @unique_id
    end

    def requires(*jobs)
      jobs.flatten.each do |job|
        raise(ArgumentError, "Must be a Dagqueue::Task") unless job.is_a?(Task)
        @dependencies << job unless self == job
      end
    end

    alias :depends_on :requires
    
    def requirements
      @dependencies.to_a.freeze
    end

    def dag=(dag)
      payload['dag_id'] = dag.unique_id
      payload['class'] = self.class.name unless payload.has_key?('class')
      @redis.set Task.job_key(unique_id), encode(payload)
    end

    def dag_id
      payload['dag_id']
    end

    # Creates a job by placing it on a queue. Expects a string queue
    # name, a string class name, and an optional array of arguments to
    # pass to the class' `perform` method.
    #
    # Raises an exception if no queue or class is given.
    def self.create(queue, klass, *args)
      raise NotImplementedError
    end

    # Given a string queue name, returns an instance of Resque::Job
    # if any jobs are available. If not, returns nil.
    def self.reserve(queue)
      raise NotImplementedError
      #return unless payload = Resque.pop(queue)
      #new(queue, payload)
    end

    # Creates an identical job, essentially placing this job back on
    # the queue.
    def recreate
      raise NotImplementedError
      #self.class.create(queue, payload_class, *args)
    end

    # String representation
    def inspect
      obj = @payload
      "(Task{%s} | %s | %s | <#%s:%s>)" % [@unique_id, obj['class'], obj['args'].inspect, self.class, self.object_id]
    end

    # Equality
    def ==(other)
      return false unless other.is_a?(Task)
      unique_id == other.unique_id
    end

    def <=>(other)
      other_id = other.unique_id rescue other.to_s
      self.unique_id <=> other_id
    end


    # Attempts to perform the work represented by this job instance.
    # Calls #perform on the class given in the payload with the
    # arguments given in the payload.
    def perform
      dag      = Dag.find(dag_id)
      job      = payload_class
      job_args = args || []

      begin
        job.perform(*job_args)
        dag.completed(unique_id)
        return true # Return true if the job was performed
                    # If an exception occurs during the job execution re-raise.
      rescue Object => e
        raise e
      end
    end

    # Returns the actual class constant represented in this job's payload.
    def payload_class
      @payload_class ||= constantize(@payload['class'])
    end

    # Returns an array of args represented in this job's payload.
    def args
      @payload['args']
    end

    # Given an exception object, hands off the needed parameters to
    # the Failure module.
    def fail(exception)
      Resque::Failure.create \
        :payload   => payload,
        :exception => exception,
        :worker    => worker,
        :queue     => queue
    end
  end
end
