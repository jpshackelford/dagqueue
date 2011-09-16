require 'spec_helper'

describe Dagqueue do

  #
  # Functional tests
  #

  class SuccessfulJob < Dagqueue::Task
    def self.perform(*args)
    end
  end

  context "given one dag with a successful job" do

    let(:job) { SuccessfulJob.new }
    let(:dag) { Dagqueue::Dag.new([job], :a_queue) }
    let(:worker) { worker = Dagqueue::Worker.new('a_queue::dags') }

    before do
      Dagqueue.enqueue(dag)
      worker.work(0)
    end

    it "marks the job complete" do
      dag.should be_job_complete(job.unique_id)
    end

    it "marks the dag complete" do
      dag.should be_complete
    end

  end

  context "given a dag with two successful jobs" do

    before do
      @job_log = []
      @tasks    = Array.new(2) { SuccessfulJob.new }
      @tasks[0].requires(@tasks[1])
      @dag             = Dagqueue::Dag.new(@tasks, :a_queue)
      worker           = Dagqueue::Worker.new('a_queue::dags')
      worker.cant_fork = true
      worker.very_verbose = true
      Dagqueue.enqueue(@dag)
      worker.work(0) { |j| @job_log << j }
    end

    it "executes jobs in dependency order" do
      @job_log.should == @tasks.reverse
    end

    it "marks the dag complete" do
      @dag.should be_complete
    end
  end

  context "given a dag with a failing and a successful job" do
    it "skips dependent jobs"
    it "marks the dag complete"
  end

  context "given a dag with a failing job" do
    it "marks the job failed"
    it "marks the dag complete"
  end

  #
  # Unit Tests
  #

  it "ordinarily assumes we are trying to enqueue a Resque::Job" do
    o = Object.new
    Resque.should_receive(:enqueue).with(o)

    Dagqueue.should respond_to(:enqueue)
    Dagqueue.enqueue(o)
  end

  it "enqueues a Dag" do
    dag = Dagqueue::Dag.new([], 'a_queue_name')
    Dagqueue.enqueue(dag)

    dag_id = DagQueue.list_dags('a_queue_name').first
    dag_id.should == dag.unique_id
  end

end


