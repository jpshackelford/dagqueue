module Dagqueue

  # These functions are for internal use only. Using these directly can
  # corrupt the Dagqueue datastore, cause workers to lock up, and the earth to
  # open under your chair and swallow you alive.
  module RedisFunctions

    PREFIX  = 'dagq:dag:'
    LIST    = PREFIX + 'list'
    COUNTER = PREFIX + 'counter'

    module DagClassMethods

      def key(unique_id, suffix = nil)
        suffix ?
            "#{PREFIX}#{unique_id}:#{suffix}" :
            "#{PREFIX}#{unique_id}"
      end

      def new_id
        Resque.redis.incr(COUNTER).to_s
      end

      def register(dag_id)
        Resque.redis.sadd(LIST, dag_id)
      end

      def list
        Resque.redis.smembers(LIST)
      end

    end

    module DagInstanceMethods

      include Resque::Helpers

      class EncodeException < StandardError;
      end

      alias resque_encode encode

      def encode(object)
        raise EncodeException, "Only Strings and Hashes valid top level " \
        "objects for JSON encoding." unless object.is_a?(Array) || object.is_a?(Hash)

        resque_encode(object)
      end

      def key(suffix = nil)
        self.class.key(unique_id, suffix)
      end

      def new_task_id
        Resque.redis.incr(key(:task_counter)).to_s
      end

      def task_key(task_id)
        key("task:#{task_id}")
      end

      def save_task(task_id, payload)
        Resque.redis.set(task_key(task_id), encode(payload))
      end

      def save_tasks(*tasks)
        Resque.redis.mset(*serialize_payloads(expand_task_keys(tasks)))
      end

      def fetch_task(task_id)
        decode Resque.redis.get(task_key(task_id))
      end

      def expand_task_keys(*tasks)
        tasks.flatten.each_slice(2).
            map { |k, v| [task_key(k), v] }.flatten
      end

      def serialize_payloads(*tasks)
        tasks.flatten.each_slice(2).
            map { |k, v| [k, encode(v)] }.flatten
      end

      def plan_tasks(*task_ids)
        task_ids.flatten.each do |task_id|
          Resque.redis.sadd(key(:tasks_planned), task_id)
        end
      end

      alias plan_task plan_tasks

      def work_task(task_id)
        Resque.redis.smove(key(:tasks_planned), key(:tasks_working), task_id)
      end

      def skip_task(task_id)
        Resque.redis.smove(key(:tasks_planned), key(:tasks_skipped), task_id)
      end

      def task_succeeded(task_id)
        Resque.redis.smove(key(:tasks_working), key(:tasks_succeeded), task_id)
      end

      def task_failed(task_id)
        Resque.redis.smove(key(:tasks_working), key(:tasks_failed), task_id)
      end

      def tasks(status = nil)
        if status
          return Resque.redis.smembers(key("tasks_#{status}"))
        else
          all_keys = [:planned, :working, :skipped, :failed, :succeeded].
              map { |s| key("tasks_#{s}") }
          return Resque.redis.sunion(* all_keys)
        end
      end

      def save_graph(graph)
        Resque.redis.set(key(:graph), encode(graph))
      end

      def fetch_graph
        decode Resque.redis.get(key(:graph))
      end

      def rgl_from_redis
        graph  = RGL::DirectedAdjacencyGraph.new
        g_hash = fetch_graph
        g_hash['edges'].flatten.each_slice(2) do |u, v|
          graph.add_edge(u, v)
        end
        g_hash['vertices'].flatten.each do |v|
          graph.add_vertex(v)
        end
        return graph
      end

      def create_tasks(vertices={ }, edges=[])
        if block_given?
          dsl = DSL.new
          yield(dsl)
          create_tasks(dsl.vertices, dsl.edges)
          return nil
        end
        raise ArgumentError, "vertices parameter must be a Hash." unless vertices.is_a?(Hash)
        raise ArgumentError, "edges parameter must be an Array" unless edges.is_a?(Array)
        id_map    = { }
        new_tasks = { }
        vertices.each_pair do |old_id, t|
          new_id = new_task_id()
          t.store('class', Task) if t['class'].nil?
          t.store('orig_id', old_id) unless old_id == new_id
          id_map.store(old_id, new_id)
          new_tasks.store(new_id, t)
        end
        task_ids = new_tasks.keys.sort
        save_tasks(* new_tasks.flatten)
        edges_real_ids = edges.flatten.map { |old| id_map[old] }
        verts_real_ids = task_ids - edges_real_ids
        save_graph('edges' => edges_real_ids, 'vertices' => verts_real_ids)
        plan_tasks(task_ids)
        return id_map
      end

    end

    class DSL

      attr_reader :vertices, :edges

      def initialize
        @unique_id = 0
        @vertices  = { }
        @edges     = []
      end

      def new_id
        (@unique_id += 1).to_s
      end

      def add_vertex(*class_and_payload)
        tclass, payload = class_and_payload.partition { |c| c.is_a?(Class) }.
            map { |e| e.first }

        h = { }
        h.store('class', tclass || Task)
        h.store('args', payload) unless payload.nil?
        id = new_id
        @vertices.store(id, h)
        return id
      end

      def add_edges(*edges)
        @edges += edges
      end

      alias add_edge add_edges

    end

  end
end