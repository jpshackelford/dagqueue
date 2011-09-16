module Dagqueue

  class Dag

    CounterKey = 'dagq:dag:counter'
    DagKey     = 'dagq:dag'

    include Resque::Helpers
    extend Resque::Helpers

    class << self

      def redis_key(unique_id)
        "#{DagKey}:#{unique_id}"
      end

      def graph_key(unique_id)
        "#{redis_key(unique_id)}:graph"
      end

      def find(unique_id)
        raise(ArgumentError, "unique_id must not be nil") if unique_id.nil?

        json = Resque.redis.get(graph_key(unique_id))

        raise(RecordNotFound, "No Dag for #{unique_id}.") if json.nil?

        graph_hash = JSON.parse(json)
        init_hash  = { 'unique_id' => unique_id }
        init_hash.merge!(graph_hash)

        return new(init_hash)
      end
    end

    def initialize(jobs = [], queue_name = 'default')

      @redis = Resque.redis

      # Users initialize with an array of jobs
      if jobs.is_a?(Array)

        if jobs.empty?
          @tasks = []
          yield self if block_given?
        else
          @tasks = jobs
        end

        @queue = queue_name

        init_redis

        # We initialize a Dag which has already been persisted
        # e.g. from Dag.find, etc. with a Hash
      elsif jobs.is_a?(Hash)
        jobs.each_pair do |var, value|
          instance_variable_set("@#{var}".to_sym, value)
        end

      else
        raise ArgumentError,
              "Dag must be initialized with an Array or Hash. " \
              "Received #{jobs.class} instead."
      end
    end

    def add_task(task_class = Task, args)
      new_task = task_class.new({:args => args}, self)
      @tasks << new_task
      new_task
    end

    def init_redis
      @tasks.each do |task|
        task.dag = self
        @redis.sadd planned_key, task.unique_id
      end
      @redis.set graph_key, graph_json
      @redis.sadd "#{DagKey}s", unique_id
    end

    private :init_redis

    def dequeue
      # Bring local graph up-to-date with completed jobs
      sweep_graph

      # Look for jobs without dependencies
      jobs_no_reqs = graph.vertices.select { |v| graph.out_degree(v) == 0 }

      # Only work a job in the planning set. This is atomic and safe for concurrency.
      jobs_no_reqs.find do |job_id|
        @redis.smove(planned_key, working_key, job_id)
      end

    end

    def completed(*job_ids)
      job_ids.flatten.each do |job_id|
        # Move the job into the completed set from whichever set it happens
        # to be in presently, e.g. it is possible to complete a job before it is
        # in a working state. Since these are atomic operations which don't raise
        # an error if there is nothing to move this is perfectly safe.
        @redis.smove(planned_key, complete_key, job_id)
        @redis.smove(working_key, complete_key, job_id)
      end

      # Now that the job isn't going to be picked up by other workers, we will
      # remove all completed jobs from our local copy of the graph. While it is possible for
      # another worker to add a new job to the completed list while we are determining what to
      # remove from the graph, the only impact of failing to remove the additional job is that
      # it is possible that the job would fulfill requirements which would allow other jobs to be
      # dequeued. This worker or another will pick up the graph update on the next loop, so this
      # is not an issue.
      sweep_graph

      # Note that completing all jobs in the Dag does not remove it from the queue.
      # That is the job of DagQueue.remove_completed( queue )
    end


    def job_complete?(job_id)
      @redis.sismember(complete_key, job_id)
    end

    # Signal that we've done all the work we need to do and this dag can be removed from its
    # Queue(s).  (We may want some code that will enforce that a Dag can only be added to a single
    # queue.
    def mark_complete

    end

    def complete?
      # TODO this doesn't properly handle the case where every node needs to complete
      # a final task.
      sweep_graph
      @redis.scard(planned_key) == 0 &&
          @redis.scard(working_key) == 0 &&
          graph.vertices.empty?
    end

    def percent_complete
      ((@redis.scard(complete_key) / graph.vertices.size.to_f) * 100).round(1)
    end

    def job_ids
      if @tasks
        return @tasks.map { |j| j.unique_id }
      elsif @vertices
        return @vertices.dup.freeze
      else
        raise "Dag was deserialized or constructed improperly."
      end
    end

    def size
      job_ids.size
    end

    def unique_id
      unless @unique_id
        @redis.setnx(CounterKey, 0)
        @unique_id = @redis.incrby(CounterKey, 1).to_s
      end
      @unique_id
    end

    def queue
      @queue
    end

    # Equality
    def ==(other)
      return false unless other.is_a?(Dag)
      unique_id == other.unique_id
    end

    private

    def sweep_graph
      @previously_completed ||= []
      newly_complete        = @redis.smembers(complete_key) - @previously_completed
      newly_complete.each do |v|
        graph.remove_vertex(v)
      end
      @previously_completed += newly_complete
    end

    def redis_key
      Dag.redis_key(unique_id)
    end

    def graph_key
      "#{redis_key}:graph"
    end

    def planned_key
      "#{redis_key}:planned"
    end

    def working_key
      "#{redis_key}:working"
    end

    def complete_key
      "#{redis_key}:complete"
    end

    def skipped_key
      "#{redis_key}:skipped"
    end

    def succeeded_key
      "#{redis_key}:succeeded"
    end

    def failed_key
      "#{redis_key}:failed"
    end

    def graph_json
      { 'vertices' => @tasks.map { |j| j.unique_id },
        'edges'    => graph.edges.map { |e| e.to_a },
        'queue'    => @queue
      }.to_json
    end

    def graph
      unless @graph
        if @tasks
          @graph = graph_from_objects
        else
          @graph = graph_from_ids
        end
      end
      @graph
    end

    def graph_from_ids
      g = RGL::DirectedAdjacencyGraph.new
      @edges.each do |u, v|
        g.add_edge(u, v)
      end
      (@vertices - g.vertices).each { |v| g.add_vertex(v) }
      return g
    end

    def graph_from_objects
      g = RGL::DirectedAdjacencyGraph.new
      @tasks.each do |j|
        j.requirements.each do |r|
          g.add_edge(j.unique_id, r.unique_id)
        end
        g.add_vertex(j.unique_id)
      end
      return g
    end

  end
end
