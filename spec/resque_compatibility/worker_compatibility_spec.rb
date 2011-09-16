require File.expand_path('../../spec_helper', __FILE__)
require File.expand_path('../resque_spec_helper', __FILE__)

describe Dagqueue::Worker do

  include Dagqueue

  before(:each) do
    Resque.before_first_fork = nil
    Resque.before_fork       = nil
    Resque.after_fork        = nil

    @worker = Dagqueue::Worker.new(:jobs)
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
  end

  it "can fail jobs" do
    Resque::Job.create(:jobs, BadJob)
    @worker.work(0)
    Resque::Failure.count.should == 1
  end

  it "failed jobs report exception and message" do
    Resque::Job.create(:jobs, BadJobWithSyntaxError)
    @worker.work(0)
    Resque::Failure.all['exception'].should == 'SyntaxError'
    Resque::Failure.all['error'].should == 'Extra Bad job!'
  end

  it "does not allow exceptions from failure backend to escape" do
    job = Resque::Job.new(:jobs, { })
    with_failure_backend BadFailureBackend do
      @worker.perform job
    end
  end

  it "fails uncompleted jobs on exit" do
    job = Resque::Job.new(:jobs, [GoodJob, "blah"])
    @worker.working_on(job)
    @worker.unregister_worker
    Resque::Failure.count.should == 1
  end

  it "can peek at failed jobs" do
    10.times { Resque::Job.create(:jobs, BadJob) }
    @worker.work(0)
    Resque::Failure.count.should == 10

    Resque::Failure.all(0, 20).size.should == 10
  end

  it "can clear failed jobs" do
    Resque::Job.create(:jobs, BadJob)
    @worker.work(0)
    Resque::Failure.count.should == 1
    Resque::Failure.clear
    Resque::Failure.count.should == 0
  end

  it "catches exceptional jobs" do
    Resque::Job.create(:jobs, BadJob)
    Resque::Job.create(:jobs, BadJob)
    @worker.process
    @worker.process
    @worker.process
    Resque::Failure.count.should == 2
  end

  it "strips whitespace from queue names" do
    queues = "critical, high, low".split(',')
    worker = Dagqueue::Worker.new(*queues)
    worker.queues.should == %w( critical high low )
  end

  it "can work on multiple queues" do
    Resque::Job.create(:high, GoodJob)
    Resque::Job.create(:critical, GoodJob)

    worker = Dagqueue::Worker.new(:critical, :high)

    worker.process

    Resque.size(:high).should == 1
    Resque.size(:critical).should == 0

    worker.process
    Resque.size(:high).should == 0
  end

  it "can work on all queues" do
    Resque::Job.create(:high, GoodJob)
    Resque::Job.create(:critical, GoodJob)
    Resque::Job.create(:blahblah, GoodJob)

    worker = Dagqueue::Worker.new("*")

    worker.work(0)

    Resque.size(:high).should == 0
    Resque.size(:critical).should == 0
    Resque.size(:blahblah).should == 0
  end

  # Under RSpec the value of processed_queues is not maintained through block calls.
  # rspec-core 2.6.4 ruby 1.9.2p180.  Same exact test works as expected in same version of
  # Ruby in original test suite against  Resque 1.17.1.
  it "processes * queues in alphabetical order" do

    pending "This test fails in MRI (not JRuby) likely due an issue with RSpec"

    Resque::Job.create(:high, GoodJob)
    Resque::Job.create(:critical, GoodJob)
    Resque::Job.create(:blahblah, GoodJob)

    worker = Dagqueue::Worker.new("*")

    processed_queues = []

    worker.work(0) do |job|
      processed_queues << job.queue
    end

    processed_queues.should == %w( jobs high critical blahblah ).sort
  end

  it "has a unique id" do
    @worker.to_s.should == "#{`hostname`.chomp}:#{$$}:jobs"
  end

  it "complains if no queues are given" do
    expect { Dagqueue::Worker.new }.to raise_error(Resque::NoQueueError)
  end

  it "fails if a job class has no `perform` method" do
    worker = Dagqueue::Worker.new(:perform_less)
    Resque::Job.create(:perform_less, Object)

    Resque::Failure.count.should == 0
    worker.work(0)
    Resque::Failure.count.should == 1
  end

  it "inserts itself into the 'workers' list on startup" do
    @worker.work(0) do
      Resque.workers[0].should == @worker
    end
  end

  it "removes itself from the 'workers' list on shutdown" do
    @worker.work(0) do
      Resque.workers[0].should == @worker
    end

    Resque.workers.should be_empty
  end

  it "removes worker with stringified id" do
    @worker.work(0) do
      worker_id = Resque.workers[0].to_s
      Resque.remove_worker(worker_id)
      Resque.workers.should be_empty
    end
  end

  it "records what it is working on" do
    @worker.work(0) do
      task = @worker.job
      task['payload'].should == { "args"=>[20, "/tmp"], "class"=>"SomeJob" }
      task['run_at'].should_not be_nil
      task['queue'].should == 'jobs'
    end
  end

  it "clears its status when not working on anything" do
    @worker.work(0)
    @worker.job.should be_empty
  end

  it "knows when it is working" do
    @worker.work(0) do
      @worker.should be_working
    end
  end

  it "knows when it is idle" do
    @worker.work(0)
    @worker.should be_idle
  end

  it "knows who is working" do
    @worker.work(0) do
      Resque.working.should == [@worker]
    end
  end

  it "keeps track of how many jobs it has processed" do
    Resque::Job.create(:jobs, BadJob)
    Resque::Job.create(:jobs, BadJob)

    3.times do
      job = @worker.reserve
      @worker.process job
    end
    @worker.processed.should == 3
  end

  it "keeps track of how many failures it has seen" do
    Resque::Job.create(:jobs, BadJob)
    Resque::Job.create(:jobs, BadJob)

    3.times do
      job = @worker.reserve
      @worker.process job
    end
    @worker.failed.should == 2
  end

  it "stats are erased when the worker goes away" do
    @worker.work(0)
    @worker.processed.should == 0
    @worker.failed.should == 0
  end

  it "knows when it started" do
    time = Time.now
    @worker.work(0) do
      @worker.started.to_s.should == time.to_s
    end
  end

  it "knows whether it exists or not" do
    @worker.work(0) do
      Dagqueue::Worker.exists?(@worker).should be_true
      Dagqueue::Worker.exists?('blah-blah').should be_false
    end
  end

  it "sets $0 while working" do
    @worker.work(0) do
      ver = Resque::Version
      $0.should == "resque-#{ver}: Processing jobs since #{Time.now.to_i}"
    end
  end

  it "can be found" do
    @worker.work(0) do
      found = Dagqueue::Worker.find(@worker.to_s)
      found.to_s.should == @worker.to_s
      found.should be_working
      found.job.should == @worker.job
    end
  end

  it "doesn't find fakes" do
    @worker.work(0) do
      found = Dagqueue::Worker.find('blah-blah')
      found.should be_nil
    end
  end

  it "cleans up dead worker info on start (crash recovery)" do
    # first we fake out two dead workers
    workerA = Dagqueue::Worker.new(:jobs)
    workerA.instance_variable_set(:@to_s, "#{`hostname`.chomp}:1:jobs")
    workerA.register_worker

    workerB = Dagqueue::Worker.new(:high, :low)
    workerB.instance_variable_set(:@to_s, "#{`hostname`.chomp}:2:high,low")
    workerB.register_worker

    Resque.workers.size.should == 2

    # then we prune them
    @worker.work(0) do
      Resque.workers.size.should == 1
    end
  end

  it "Processed jobs count" do
    @worker.work(0)
    Resque.info[:processed].should == 1
  end

  it "Will call a before_first_fork hook only once" do
    Resque.redis.flushall
    $BEFORE_FORK_CALLED      = 0
    Resque.before_first_fork = Proc.new { $BEFORE_FORK_CALLED += 1 }
    workerA                  = Dagqueue::Worker.new(:jobs)
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')

    $BEFORE_FORK_CALLED.should == 0

    workerA.work(0)

    $BEFORE_FORK_CALLED.should == 1

    # TODO: Verify it's only run once. Not easy.
    #     workerA.work(0)
    #     assert_equal 1, $BEFORE_FORK_CALLED
  end

  it "Will call a before_fork hook before forking" do
    Resque.redis.flushall
    $BEFORE_FORK_CALLED = false
    Resque.before_fork  = Proc.new { $BEFORE_FORK_CALLED = true }
    workerA             = Dagqueue::Worker.new(:jobs)

    $BEFORE_FORK_CALLED.should be_false
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
    workerA.work(0)
    $BEFORE_FORK_CALLED.should be_true
  end

  it "very verbose works in the afternoon" do

    Time.should_receive(:now).any_number_of_times().
        and_return( Time.parse("15:44:33 2011-03-02") )

    @worker.should_receive(:puts).with(
        match(/\*\* \[15:44:33 2011-03-02\] \d+: some log text/) )

    @worker.very_verbose = true
    @worker.log("some log text")
  end
  
  # Under RSpec the value of processed_queues is not maintained through block calls.
  # rspec-core 2.6.4 ruby 1.9.2p180.  Same exact test works as expected in same version of
  # Ruby in original test suite against  Resque 1.17.1.
  it "Will call an after_fork hook after forking" do

    pending "This test fails in MRI (not JRuby) likely due an issue with RSpec"

    Resque.redis.flushall
    $AFTER_FORK_CALLED = false
    Resque.after_fork  = Proc.new { $AFTER_FORK_CALLED = true }
    workerA            = Dagqueue::Worker.new(:jobs)

    $AFTER_FORK_CALLED.should be_false
    Resque::Job.create(:jobs, SomeJob, 20, '/tmp')
    workerA.work(0)
    $AFTER_FORK_CALLED.should be_true
  end

  it "returns PID of running process" do
    @worker.pid.should == @worker.to_s.split(":")[1].to_i
  end

end