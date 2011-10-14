require 'spec_helper'

include Dagqueue::RedisFunctions

# Specifying very low-level details about how objects are stored in Redis


describe DagClassMethods do

  include DagClassMethods

  it "increments unique id by one on successive calls" do
    3.times do |n|
      new_id.should == (n + 1).to_s
    end
  end

  it "keeps a list of active Dags" do
    3.times { |n| register(n + 1) }
    list.should == %w{1 2 3}
  end

  it "allocates Dag unique id with INCR counter" do
    Resque.redis.should_receive(:incr).with(COUNTER).and_return { 123 }
    new_id.should == 123.to_s
  end

  it "registers the Dag with SADD list" do
    Resque.redis.should_receive(:sadd).with(LIST, 123)
    register(123)
  end

  it "lists registered Dags with SMEMBERS list" do
    Resque.redis.should_receive(:smembers).with(LIST).
        and_return(%w{1 2 3})
    list
  end

  it "calculates Dag specific keys" do
    key(123, :tasks_planned).should == PREFIX + 123.to_s + ':tasks_planned'
  end

end

describe DagInstanceMethods do

  class FakeDag

    extend DagClassMethods
    include DagInstanceMethods

    attr_accessor :unique_id

    def initialize(dag_id)
      @unique_id = dag_id
    end

  end

  let(:dag) { FakeDag.new(123) }

  it "expands task keys" do
    dag.expand_task_keys(456, 'payload1', 678, 'payload2').should ==
        %w{dagq:dag:123:task:456 payload1 } +
            %w{dagq:dag:123:task:678 payload2 }
  end

  it "persists task payloads in bulk" do
    dag.save_tasks(456, { :args => 'payload1' }, 678, { :args => 'payload2' })
    dag.fetch_task(456).should == { 'args' => 'payload1' }
    dag.fetch_task(678).should == { 'args' => 'payload2' }
  end

  it "serializes task payloads in JSON" do
    dag.serialize_payloads(456, { :class => Task, :arg => 'payload1' },
                           678, { :class => Task, :arg => 'payload2' }).should ==
        [456, "{\"class\":\"Dagqueue::Task\",\"arg\":\"payload1\"}"] +
            [678, "{\"class\":\"Dagqueue::Task\",\"arg\":\"payload2\"}"]
  end

  context "when creating tasks" do

    it "raises an error unless first parameter is a Hash" do
      expect { dag.create_tasks([], []) }.to raise_error(ArgumentError)
    end

    it "raises an error unless second parameter is an Array" do
      expect { dag.create_tasks({ }, { }) }.to raise_error(ArgumentError)
    end

    context "with core API" do

      it "saves tasks and graph in Redis and marks tasks as planned" do
        # order is very important for this to be safe across many processes.
        id = 0
        dag.should_receive(:new_task_id).exactly(4).times.and_return { (id += 1).to_s }
        dag.should_receive(:save_tasks).with(
            '1', { 'class' => Task, 'args' => 'payload1', 'orig_id' => :a },
            '2', { 'class' => Class, 'args' => 'payload2', 'orig_id' => :b },
            '3', { 'class' => Task, 'orig_id' => :c },
            '4', { 'class' => Task, 'args' => 'payload4', 'orig_id' => :d }
        ).ordered
        dag.should_receive(:save_graph).with({ 'edges' => %w{2 1 3 1}, 'vertices' => ['4'] }).ordered
        dag.should_receive(:plan_tasks).with(%w{1 2 3 4}).ordered

        # exercise code
        id_map = dag.create_tasks({ :a => { 'class' => Task, 'args' => 'payload1' },
                                    :b => { 'class' => Class, 'args' => 'payload2' },
                                    :c => { 'class' => Task },
                                    :d => { 'args' => 'payload4' } },
                                  [:b, :a, :c, :a])

        id_map.should == { :a => '1', :b => '2', :c => '3', :d => '4' }
      end
    end

    context "with DSL" do
      it "saves tasks and graph in Redis and marks tasks as planned" do
        id = 0
        dag.should_receive(:new_task_id).exactly(4).times.and_return { (id += 1).to_s }
        dag.should_receive(:save_tasks).with(
            '1', { 'class' => Task, 'args' => 'payload1' },
            '2', { 'class' => Class, 'args' => 'payload2' },
            '3', { 'class' => Task },
            '4', { 'class' => Task, 'args' => 'payload4' }
        ).ordered
        dag.should_receive(:save_graph).with({ 'edges' => %w{2 1 3 1}, 'vertices' => ['4'] }).ordered
        dag.should_receive(:plan_tasks).with(%w{1 2 3 4}).ordered

        # exercise code
        ret = dag.create_tasks do |dag|
          v1 = dag.add_vertex('payload1', Task)
          v2 = dag.add_vertex('payload2', Class)
          v3 = dag.add_vertex(Task)
          v4 = dag.add_vertex('payload4')
          dag.add_edges(v2, v1, v3, v1)
        end
        ret.should be_nil
      end
    end

  end

  describe RedisFunctions::DSL do

    let(:dsl) { RedisFunctions::DSL.new }

    it "adds a vertex with payload and class" do
      dsl.add_vertex(Class, 'payload').should == '1'
      dsl.vertices.should == { '1' => { 'class' => Class, 'args' => 'payload' } }
    end

    it "adds a vertex with payload only" do
      dsl.add_vertex('payload').should == '1'
      dsl.vertices.should == { '1' => { 'class' => Task, 'args' => 'payload' } }
    end

    it "adds a vertex with class only" do
      dsl.add_vertex(Class).should == '1'
      dsl.vertices.should == { '1' => { 'class' => Class } }
    end

    it "adds edges" do
      dsl.add_edges(:b, :a, :c, :a)
      dsl.add_edge(:d, :a)
      dsl.edges.should == [:b, :a, :c, :a, :d, :a]
    end
  end

  it "allocates a task unique id with INCR task_counter" do
    Resque.redis.should_receive(:incr).with(Dag.key(123, :task_counter)).
        and_return(456)

    dag.new_task_id.should == 456.to_s
  end

  it "adds tasks as planned with SADD task_planned" do
    Resque.redis.should_receive(:sadd).with(Dag.key(123, :tasks_planned), '456')
    dag.plan_task('456')
  end

  it "adds multiple tasks as planned with SADD task_planned" do
    Resque.redis.should_receive(:sadd).with(Dag.key(123, :tasks_planned), '456').once
    Resque.redis.should_receive(:sadd).with(Dag.key(123, :tasks_planned), '789').once

    dag.plan_tasks(%w{456 789})
  end


  context "when a task is planned" do

    it "marks it in-progress with SMOVE" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_planned),
          Dag.key(123, :tasks_working),
          '456').once.and_return true

      dag.work_task('456').should be_true
    end

    it "marks it skipped with SMOVE" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_planned),
          Dag.key(123, :tasks_skipped),
          '456').once.and_return true

      dag.skip_task('456').should be_true
    end

    it "does not mark it succeeded" do

      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_working),
          Dag.key(123, :tasks_succeeded),
          '456').once.and_return false

      dag.task_succeeded('456').should be_false
    end

    it "does not mark it failed" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_working),
          Dag.key(123, :tasks_failed),
          '456').once.and_return false

      dag.task_failed('456').should be_false
    end

  end

  context "when a task is in-progress" do

    it "marks it succeeded with SMOVE" do

      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_working),
          Dag.key(123, :tasks_succeeded),
          '456').once.and_return true

      dag.task_succeeded('456').should be_true
    end

    it "marks it failed with SMOVE" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_working),
          Dag.key(123, :tasks_failed),
          '456').once.and_return true

      dag.task_failed('456').should be_true
    end

    it "does not mark it in-progress" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_planned),
          Dag.key(123, :tasks_working),
          '456').once.and_return false

      dag.work_task('456').should be_false
    end

    it "does not mark it skipped" do
      Resque.redis.should_receive(:smove).with(
          Dag.key(123, :tasks_planned),
          Dag.key(123, :tasks_skipped),
          '456').once.and_return false

      dag.skip_task('456').should be_false
    end

  end

  it "lists tasks in a given state with SMEMBERS" do
    Resque.redis.should_receive(:smembers).with(Dag.key(123, :tasks_a_state)).
        and_return(%w{1 2 3})
    dag.tasks(:a_state).should == %w{ 1 2 3 }
  end

  it "lists tasks in all states with SUNION" do

    all_keys = [:planned, :working, :skipped, :failed, :succeeded].
        map { |s| dag.key("tasks_#{s}") }

    Resque.redis.should_receive(:sunion).with(* all_keys).
        and_return(%w{1 2 3})

    dag.tasks.should == %w{ 1 2 3 }
  end

  it "lists tasks in all states" do
    dag.plan_tasks(%w{1 2 3})
    dag.tasks.should == %w{1 2 3}
  end

  let(:a_graph) { a_graph = { 'edges' => [2, 1, 3, 1, 4, 1], 'vertices' => [5, 6] } }

  it "persists the graph with SET graph" do
    Resque.redis.should_receive(:set).with(Dag.key(123, :graph), dag.encode(a_graph))
    dag.save_graph a_graph
  end

  it "fetches the graph with GET graph" do
    Resque.redis.should_receive(:get).with(Dag.key(123, :graph)).
        and_return(dag.encode(a_graph))
    dag.fetch_graph.should == a_graph
  end

  it "round-trips graph data" do
    g_hash = {'edges' => %w{2 1 3 1}, 'vertices' => ['4']}
    dag.save_graph g_hash
    dag.fetch_graph.should == g_hash
  end

  it "retreives a graph from redis" do

    graph = RGL::DirectedAdjacencyGraph.new
    graph.add_edge('2', '1')
    graph.add_edge('3', '1')
    graph.add_vertex('4')

    dag.save_graph('edges' => %w{2 1 3 1}, 'vertices' => ['4'])

    dag.rgl_from_redis.should == graph
  end

  it "calculates task specific keys" do
    dag.task_key(456).should ==
        PREFIX + 123.to_s + ':task:' + 456.to_s
  end

  it "stores task payload with SET task_payload" do
    Resque.redis.should_receive(:set).with(
        dag.task_key(456), dag.encode(:args => 'a_payload'))
    dag.save_task(456, :args => 'a_payload')
  end

  it "stores multiple task payloads with MSET task_payloads" do
    Resque.redis.should_receive(:mset).with(
        dag.task_key(456), dag.encode(:args => 'payload1'),
        dag.task_key(789), dag.encode(:args => 'payload2'))

    dag.save_tasks(456, { :args => 'payload1' },
                   789, { :args => 'payload2' })
  end

  it "fetches the task payload with GET task_payload" do
    Resque.redis.should_receive(:get).with(dag.task_key(456)).
        and_return(dag.encode(:args => 'a_payload'))

    dag.fetch_task(456).should == { 'args' => 'a_payload' }
  end

  it "raises an error if one attempts to encode a string" do
    # See http://redmine.ruby-lang.org/issues/2160
    expect { dag.encode('payload') }.to raise_error(DagInstanceMethods::EncodeException)
  end

end
