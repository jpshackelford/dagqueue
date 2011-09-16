module Dagqueue

  class DagQueue

    DagQueueKey = 'dag_queue'


    class << self

      def resque_compatible?
        false
      end

      def list_queues
        Resque.redis.smembers(queues_key).sort
      end


      def list_dags(*queue_names)
        dags = []
        # TODO Can we do this more efficiently with a different data structure?
        queue_names.flatten.each do |queue_name|
          dags += Resque.redis.zrange queue_key(queue_name), 0, -1
        end
        dags
      end

      def create_queue(*queue_names)
        queue_names.flatten.each do |queue_name|
          Resque.redis.sadd queues_key, queue_name
        end
      end

      def enqueue(queue_name, *dags)
        create_queue(queue_name)
        dags.flatten.each do |dag|
          Resque.redis.zadd queue_key(queue_name), next_score, dag.unique_id
        end
      end

      def size(queue_name)
        Resque.redis.zcard queue_key(queue_name)
      end

      alias :create_queues :create_queue

      # TODO Current implementation is O(n2) and needs revisiting.
      def remove_completed(*queues)

        rm_queues = queues.flatten
        rm_queues = list_queues if rm_queues.empty?

        rm_queues.each do |queue|
          list_dags(queue).each do |dag_id|
            dag = Dag.find(dag_id)
            remove_dag(queue, dag) if dag.complete?
          end
        end
      end

      def remove_dag(queue, dag)
        Resque.redis.zrem queue_key(queue), dag.unique_id
      end

      def reserve(queue_name)
        # find next
        dag_id = Resque.redis.zrange(queue_key(queue_name), 0, 0).shift
        dag    = Dag.find(dag_id) if dag_id

        task    = nil
        task_id = dag.dequeue if dag
        # We are guaranteed that if we have a job_id at this point,, no other worker has it.
        
        task = Task.find(task_id) if task_id
        return task
      end

      private

      def next_score
        Resque.redis.incr queue_counter_key
      end

      def queue_counter_key
        "#{DagQueueKey}s:counter"
      end

      def queues_key
        "#{DagQueueKey}s"
      end

      def queue_key(name)
        "#{DagQueueKey}:#{name}"
      end

    end


  end

end