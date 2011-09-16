require 'spec_helper'

include Dagqueue

require 'logger'

describe DagQueue do

  it "is not Resque compatible" do
    DagQueue.should_not be_resque_compatible
  end

  it "lists queued dags sorted alphabetically" do
    DagQueue.create_queues(%w{critical high med low })
    DagQueue.list_queues.should == %w{critical high low med}
  end

  context "when enqueuing a dag" do

    let(:dags) { Array.new(3) { Dag.new } }

    before do
      DagQueue.enqueue(:a_queue, dags)
    end

    it "creates a queue" do
      DagQueue.list_queues.should == ['a_queue']
    end

    it "populates the queue with the dags" do
      DagQueue.list_dags('a_queue').should == dags.map { |dag| dag.unique_id }
    end

    it "counts the dags in the queue" do
      DagQueue.size('a_queue').should == 3
    end

  end

  context "when all jobs are complete" do

    before do
      dags = Array.new(4) do
        Dag.new(Array.new(3) { Task.new })
      end
      DagQueue.enqueue(:a_queue, dags[0..1])
      DagQueue.enqueue(:b_queue, dags[2..3])
      dags.each { |dag| dag.completed(dag.job_ids) }
    end

    it "removes dags from all queues" do
      DagQueue.remove_completed
      DagQueue.size(:a_queue).should == 0
      DagQueue.size(:b_queue).should == 0
    end

    it "it removes dags from a specified queue when all the jobs are complete" do
      DagQueue.remove_completed(:a_queue)
      DagQueue.size(:a_queue).should == 0
      DagQueue.size(:b_queue).should == 2
    end

  end

  context "when reserving or dequeuing" do

    before do

      # Generate 3 dags with 3 jobs each, where each job depends on another
      # such that there are no circular dependencies and the execution order
      # may easily be predicted but not by their creation order or unique_ids.
      # We want our tests not to give us false positives if they were to use
      # creation order or ids for their calculations.

      tasks = Array.new(9) { Dagqueue::Task.new }
      tasks.each { |task| task.unique_id }
      tasks.sort_by! { rand }

      # Within each Dag one job depends on another.
      tasks.each_with_index do |job, n|
        tasks[n].requires(tasks[n+1]) unless (n+1) % 3 == 0
      end

      dags = Array.new(3) do |n|
        Dag.new(tasks.slice(n*3..n*3+2))
      end

      # Queue up the Dags and set the execution order our tests will use for assertions
      DagQueue.enqueue(:a_queue, dags.reverse)
      @execution_order = tasks.reverse # since Dags were reversed
    end

    it "returns a task not a Dag" do
      DagQueue.should respond_to(:reserve)
      DagQueue.reserve(:a_queue).should be_a(Dagqueue::Task)
    end

    it "returns task from the first Dag on the queue, if one is available" do
      task = DagQueue.reserve(:a_queue)
      task.should == @execution_order.first
    end

    it "a task may be dequeued once and only once and in dependency order" do
      dequeued_tasks = Array.new(9) do
        task = DagQueue.reserve(:a_queue)
        Dag.find(task.dag_id).completed(task.unique_id) if task
        DagQueue.remove_completed(:a_queue)
        task
      end
      dequeued_tasks.should == @execution_order
    end

    it "returns nil when the queue doesn't exist " do
      DagQueue.reserve(:nosuch_queue).should be_nil
    end

    context "when configured for no-skip-ahead" do
      it "looks for jobs in other dags if the current dag has no jobs available"
    end

    context "when configured for skip-ahead" do
      it "does not look for jobs in the next dag until the current dag is complete"
    end

  end


end