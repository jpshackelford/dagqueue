module Dagqueue

  # subclass Resque's worker override the following:
  # - #reserve
  # - #queues

  # look at:
  # - find

  # job is assumed to have a #queue method in:
  # - working_on
  # - work

  # unregister_worker  tries to create a job

  class Worker < Resque::Worker

    class << self

      def default_queue_type
        Resque::JobQueue
      end

      def queue_typemap
        init_queue_typemap
        @queue_typemap.dup
      end

      def queue_type_for(name)
        init_queue_typemap
        @queue_typemap[name]
      end

      def add_queue_type(queue_class, queue_alias)
        init_queue_typemap
        @queue_typemap.store(queue_alias.to_s, queue_class)
      end

      private

      def init_queue_typemap
        @queue_typemap ||= { 'jobs' => Resque::JobQueue,
                             'dags' => Dagqueue::DagQueue }
      end

    end

    # Initialize a workers with an array of Strings representing queue names.
    # A suffix to the name indicates the queue type, e.g. a_name::jobs, or
    # another_name::dags). Names unadorned with a type suffix are  assumed
    # to be standard Resque job queues to preserve compatibility with existing
    # Resque installations. (In other words, the Dagqueue worker should be a
    # drop in replacement for Resque's worker.)
    #
    # The order is important: a Worker will check the first
    # queue given for a job. If none is found, it will check the
    # second queue name given. If a job is found, it will be
    # processed. Upon completion, the Worker will again check the
    # first queue given, and so forth. In this way the queue list
    # passed to a Worker on startup defines the priorities of queues.
    #
    # If passed a single "*", this Worker will operate on all Resque Job
    # queues in alphabetical order, just as if this were a typical
    # Resque worker. Queues can be dynamically added or
    # removed without needing to restart workers using this method.
    #
    # To check queues of a particular type use the syntax "*<queue type>"
    # e.g. "*dags". (The single asterisk is actually shorthand for "*jobs").
    # To check all queues of all types use "**".
    #
    #  Suffixed queue names are mapped to types with Worker#queue_type_for( name ).
    # New types may be added with Worker#add_type.  Libraries adding new queue types
    # should first require dagqueue and then call Worker#add_type. No support for
    # adding requiring additional libraries currently exists in the dagqueue daemon.
    def initialize(*queues)
      @queues_with_types = []
      @queues            = []
      @wildcards         = []

      queues.flatten.each do |queue|
        parts = queue.to_s.strip.split('::')
        name  = parts.shift.strip

        # wildcard processing
        if name == '**'
        end

        if name == '*'
          name = '*jobs'
        end

        if name =~ /^\*([\w\*]*)/
          type_name = $1
          @wildcards << Worker.queue_type_for(type_name)
          next
        end

        # queue name processing
        if parts.empty?
          @queues << name
          queue_type = Worker.default_queue_type
        else
          queue_type = Worker.queue_type_for(parts.join('::'))
          if queue_type.resque_compatible?
            @queues << name
          end
        end
        @queues_with_types << { :name => name, :type => queue_type }
      end

      validate_queues
    end

    # A worker must be given a queue, otherwise it won't know what to
    # do with itself.
    #
    # You probably never need to call this.
    def validate_queues
      if (@queues.nil? || @queues.empty?) &&
          (@wildcards.nil? || @wildcards.empty?) &&
          (@queues_with_types.nil? || @queues_with_types.empty?)
        raise Resque::NoQueueError.new("Please give each worker at least one queue.")
      end
    end

    def queues_to_check
      unless @wildcards.empty?

        @wildcards.map do |queue_type|
          queue_type.list_queues.map do |queue_name|
            { :name => queue_name, :type => queue_type }
          end
        end.flatten

      else
        queues_with_types
      end
    end

    # Attempts to grab a job off one of the provided queues. Returns
    # nil if no job can be found.
    def reserve
      queues_to_check.each do |queue|
        log! "Checking #{queue[:name]}"
        if job = queue[:type].reserve(queue[:name])
          log! "Found job on #{queue}"

          return job
        end
      end

      nil
    rescue Exception => e
      log "Error reserving job: #{e.inspect}"
      log e.backtrace.join("\n")
      raise e
    end

    # Returns a list of queues to use when searching for a job.
    # A splat ("*") means you want every queue (in alpha order) - this
    # can be useful for dynamically adding new queues. Note that
    # this method only returns traditional Resque job queues. For a complete
    # list of queues, #queues_with_type.
    def queues
      @queues[0] == "*" ? Resque.queues.sort : @queues
    end

    # List of all queues including type info. The Hash returned is in the form
    # { :name => 'a_queue'', :type => SomeQueueClass}
    def queues_with_types
      @queues_with_types.dup
    end

    ## Processes a given job in the child.
    #def perform(job)
    #  begin
    #    run_hook :after_fork, job
    #    job.perform
    #    #job.record_success if job.respond_to?( :record_success )
    #  rescue Object => e
    #    log "#{job.inspect} failed: #{e.inspect}"
    #    #job.record_failure if job.respond_to?( :record_failure )
    #    begin
    #      job.fail(e)
    #    rescue Object => e
    #      log "Received exception when reporting failure: #{e.inspect}"
    #    end
    #    failed!
    #  else
    #    log "done: #{job.inspect}"
    #  ensure
    #    yield job if block_given?
    #  end
    #end


  end
end