require 'spec_helper'

include Dagqueue

describe Dag do

  before(:each) do
    @tasks = Array.new(4) { Task.new }
    @tasks[1].requires(@tasks.first)
    @tasks[2].requires(@tasks.first)
  end

  shared_examples_for 'a Dag' do

    it "has associated jobs" do
      ids = @tasks.map { |j| j.unique_id }.sort
      @dag.job_ids.sort.should == ids
    end

    it "has a size" do
      @dag.size.should == 4
    end

    it "has a graph of jobs" do
      graph = @dag.send(:graph)
      graph.vertices.sort.should == @tasks.map { |j| j.unique_id }.sort
      edges = graph.edges.map { |e| e.to_a }
      edges.should == [[@tasks[1].unique_id, @tasks[0].unique_id],
                       [@tasks[2].unique_id, @tasks[0].unique_id]].sort
    end

    it "initially has same planned jobs as the vertices" do
      key     = @dag.send(:planned_key)
      graph   = @dag.send(:graph)
      planned = Resque.redis.smembers(key)

      planned.sort.should == graph.vertices.sort
    end

    it "dequeues a job" do
      # should return a vertex without an outbound edge
      jobs_without_reqs = [@tasks.first.unique_id, @tasks.last.unique_id]
      job_id            = @dag.dequeue
      jobs_without_reqs.should include(job_id)

      # should move the vertex to the working set
      planned = Resque.redis.sismember(@dag.send(:planned_key), job_id)
      working = Resque.redis.sismember(@dag.send(:working_key), job_id)

      planned.should be_false
      working.should be_true
    end

    it "does not dequeue a job that is being worked" do
      jobs_without_reqs = [@tasks.first.unique_id, @tasks.last.unique_id]

      job1 = @dag.dequeue
      job2 = @dag.dequeue

      job2.should_not == job1

      jobs_without_reqs.should include(job1)
      jobs_without_reqs.should include(job2)
    end

    it "does not dequeue a job which is complete" do
      completed_job = @tasks.last.unique_id
      Resque.redis.smove(@dag.send(:planned_key),
                         @dag.send(:complete_key), completed_job)
      4.times do
        @dag.dequeue.should_not == completed_job
      end
    end

    it "does not dequeue a job which has incomplete requirements" do
      2.times { @dag.dequeue }

      @dag.dequeue.should be_nil

      remaining_jobs = Resque.redis.smembers(@dag.send(:planned_key))
      remaining_jobs.should include(@tasks[1].unique_id)
      remaining_jobs.should include(@tasks[2].unique_id)
    end

    it "marks a job complete" do
      job_id = @tasks[0].unique_id
      @dag.completed(job_id)

      planned  = Resque.redis.sismember(@dag.send(:planned_key), job_id)
      working  = Resque.redis.sismember(@dag.send(:working_key), job_id)
      complete = Resque.redis.sismember(@dag.send(:complete_key), job_id)

      planned.should be_false
      working.should be_false
      complete.should be_true

      graph = @dag.send(:graph)
      graph.vertices.should_not include(job_id)
    end

    it "does dequeue a job with completed requirements" do
      # implement this test by directly smoving items into the completed set
      # to simulate another client
      job_id = @tasks.shift.unique_id
      Resque.redis.smove(@dag.send(:planned_key), @dag.send(:complete_key), job_id)

      dequeued       = Array.new(3) { @dag.dequeue }.sort
      remaining_jobs = @tasks.map { |j| j.unique_id }.sort

      dequeued.should == remaining_jobs
    end

    it "is incomplete if jobs are planned" do
      @dag.should_not be_complete
    end

    it "is incomplete if jobs are working" do
      jobs = @tasks.map { |j| j.unique_id }

      # Complete the first job (other jobs depend on it) so that all others can be queued
      @dag.completed(jobs.shift)

      # All other jobs are working and there is nothing in planned
      jobs.each { |j| @dag.dequeue }
      Resque.redis.scard(@dag.send(:planned_key)).should == 0

      # but the Dag isn't done until all jobs are complete
      @dag.should_not be_complete
    end

    it "is complete when all jobs are complete" do
      @tasks.map { |j| j.unique_id }.each { |j| @dag.completed(j) }

      # Be sure this works even when we don't rely on any state in Dag itself
      dag = Dag.find(@dag.unique_id)
      dag.should be_complete
    end

    it "calculates percent complete only on completed jobs" do
      @dag.completed(@tasks.first.unique_id) # 1 of 4 complete
      2.times { @dag.dequeue }               # working jobs don't count toward completion

      # Be sure this works even when we don't rely on any state in Dag itself
      dag = Dag.find(@dag.unique_id)
      dag.percent_complete.should == 25.0
    end

    it "skips dependent jobs when a required job fails"
    it "lists jobs which succeeded"
    it "lists jobs which failed"

    # helpful for tuning--knowing how many workers could be productively added
    it "reports the number of jobs available for work given current state of the local graph"

    it "a dag may be paused"

    it "releases memory in redis when requested, if the dag is complete"
    it "assumes dag was completed if redis data disappears"
    it "operations on a finalized dag are rational"

    it "provides hooks for plugins--especially for finalizing the dag"

  end

  context "when constructed with a block and depends_on" do

    before do
      @tasks = []
      @dag   = Dag.new do |dag|
        @tasks << dag.add_task(Task, 'payload_1')
        @tasks << dag.add_task(Task, 'payload_2')
        @tasks << dag.add_task(Task, 'payload_3')
        @tasks[0].depends_on(@tasks[1])
        @tasks[0].depends_on(@tasks[2])
      end
    end

    it "adds tasks that are associated with the dag" do
      @dag.send(:instance_variable_get, :@tasks).should == @tasks
      @tasks.first.dag_id.should == @dag.unique_id
    end

    it "maintains dependencies" do
      @tasks.first.requirements.size.should == 2
      @tasks.first.requirements.should == @tasks[1..2]
    end

  end

  context "when constructed with nested blocks" do

    before do
      @dag   = Dag.new do |dag|
        @task = dag.add_task(Task, 'payload_1') do |task|
          task.depends_on(Task, 'payload_2')
          task.depends_on(Task, 'payload_3')
        end
      end
    end

    it "adds tasks that are associated with the dag" do
      @dag.send(:instance_variable_get, :@tasks).size.should == 3
    end

    it "maintains dependencies" do
      @task.requirements.size.should == 2
      @task.requirements.each do |t|
        t.unique_id.should_not be_nil
        t.unique_id.should_not == @task.unique_id
      end
    end

    it "is properly persisted in Redis" do
      dag = Dag.find( @dag.unique_id )
      tasks = dag.send(:instance_variable_get, :@tasks).size.should == 3
      task = tasks.find{|t| t.unique_id == @task.unique_id}
      task.should_not be_nil
      task.requirements.size.should == 2
      task.requirements.each do |t|
        t.unique_id.should_not be_nil
        t.unique_id.should_not == @task.unique_id
      end
    end
  end

  context "when constructed from an Array of jobs" do

    before(:each) do
      @dag = Dag.new(@tasks)
    end

    it_behaves_like 'a Dag'

    it "obtains a unique id from redis" do
      Dag.new.unique_id.should_not == Dag.new.unique_id
    end

  end

  context "when retrieved from redis" do

    before(:each) do
      @id  = Dag.new(@tasks).unique_id
      @dag = Dag.find(@id)
    end

    it "persists graph edges" do
      key        = @dag.send(:graph_key)
      graph_json = Resque.redis.get(key)
      graph      = @dag.decode(graph_json)
      graph['edges'].should_not be_nil
      graph['edges'].should_not be_empty
    end

    it_behaves_like 'a Dag'

    it "raises an error when a dag isn't found" do
      expect { Dag.find(123) }.to raise_error(RecordNotFound)
    end

    it "raises an error when a nil is passed" do
      expect { Dag.find(nil) }.to raise_error(ArgumentError)
    end

    it "maintains the unique_id" do
      @dag.unique_id.should == @id
    end

    it "includes associated jobs" do
      @dag.job_ids.sort.should == @tasks.map { |j| j.unique_id }.sort
    end

  end

  it "if unspecified, queue name is default" do
    Dag.new.queue.should == 'default'
  end

  it "persists queue name" do
    dag = Dag.new [], 'a_queue'
    Dag.find(dag.unique_id).queue.should == 'a_queue'
  end

  it "does not consider different instances of a dag equal" do
    dag1 = Dag.new
    dag2 = Dag.new
    dag1.should_not == dag2
  end

  it "does consider the same instance of a dag equal" do
    dag1 = Dag.new
    dag1.should == dag1
  end

  it "detects circular dependencies and raises an error"

  it "doesn't allow jobs to express a dependency on a job in another DAG"

  context "given a one-job dag" do

    let(:job) { Task.new }
    let(:dag) { Dag.new([job]) }

    context "when the job is completed" do
      before { dag.completed(job.unique_id) }
      it "reports that the job is complete" do
        dag.should be_job_complete(job.unique_id)
      end
    end

    context "when the job is not completed" do
      it "reports that the job is incomplete" do
        dag.should_not be_job_complete(job.unique_id)
      end
    end

  end


end