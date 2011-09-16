require 'spec_helper'

describe Dagqueue::Worker do


  context "when initialized without suffixed queue names" do
    let(:worker) { Dagqueue::Worker.new(%w{ critical high med low}) }

    it "lists all queues" do
      worker.queues.should == %w{ critical high med low }
    end

    it "supplies type for every queue" do
      worker.queues_with_types.should ==
          [
              { :name => 'critical', :type => Resque::JobQueue },
              { :name => 'high', :type => Resque::JobQueue },
              { :name => 'med', :type => Resque::JobQueue },
              { :name => 'low', :type => Resque::JobQueue }
          ]
    end
  end

  context "when initialized with a mix of queue types" do

    let(:worker) { Dagqueue::Worker.new(%w{ queue_a queue_b::dags queue_c::jobs}) }

    it "includes only Resque job queues in the queue list" do
      worker.queues.should == %w{ queue_a queue_c }
    end

    it "supplies type for every queue" do
      worker.queues_with_types.should ==
          [
              { :name => 'queue_a', :type => Resque::JobQueue },
              { :name => 'queue_b', :type => Dagqueue::DagQueue },
              { :name => 'queue_c', :type => Resque::JobQueue }
          ]
    end
  end

  context "when initialized with added types" do


    before :each do
      CompatQueue = mock('CompatQueueClass') unless defined?(CompatQueue)
      IncompatQueue = mock('IncompatQueueClass') unless defined?(IncompatQueue)

      CompatQueue.should_receive(:resque_compatible?).and_return(true)
      IncompatQueue.should_receive(:resque_compatible?).and_return(false)
      Worker.add_queue_type(CompatQueue, 'compat')
      Worker.add_queue_type(IncompatQueue, 'incompat')
    end

    let(:worker) { Dagqueue::Worker.new(%w{ queue_a::compat queue_b::incompat }) }

    it "lists Resque compatible queues in the queue list" do
      worker.queues.should include 'queue_a'
    end

    it "doesn't list incompatible queues in the queue list" do
      worker.queues.should_not include 'queue_b'
    end

    it "supplies types for all queues" do
      worker.queues_with_types.should ==
          [
              { :name => 'queue_a', :type => CompatQueue },
              { :name => 'queue_b', :type => IncompatQueue }
          ]
    end
  end

  context "when initialized with only a Dag queue" do
    it "the worker is valid" do
      Dagqueue::Worker.new('a_queue::dags')
    end
  end

  context "when initialized with invalid queue types" do
    it "raises an error"
  end

  context "when dequeuing jobs" do

    let(:worker) { Dagqueue::Worker.new(%w{ critical high med low }) }

    it "reserves jobs using the Queue type rather than Job.reserve directly" do
      Resque::JobQueue.should_receive(:reserve).exactly(4).times
      worker.reserve
    end

    it "JobQueue.reserve delegates to Job.reserve" do
      Resque::Job.should_receive(:reserve).with('a_queue').once
      Resque::JobQueue.reserve('a_queue')
    end

    it "supports a type specific wildcard for Resque job queues" do
      Resque::Job.create(:high, GoodJob)
      Resque::Job.create(:critical, GoodJob)
      Resque::Job.create(:blahblah, GoodJob)

      worker = Dagqueue::Worker.new("*jobs")

      worker.work(0)

      Resque.size(:high).should == 0
      Resque.size(:critical).should == 0
      Resque.size(:blahblah).should == 0
    end

    xit "supports a type specific wildcard for an arbitrary queue type" do
      worker = Dagqueue::Worker.new(%w{ *a_type })
    end

    xit "allows multiple wildcards" do
      worker = Dagqueue::Worker.new(%w{ *a_queue_type *b_queue_type })
    end

    xit "supports a wildcard against all known queue types" do
      worker = Dagqueue::Worker.new("**")
    end

    xit "permits mixing of wildcards and specific queues" do
      worker = Dagqueue::Worker.new(%w{ *a_queue_type, critical, high })
    end

  end

end