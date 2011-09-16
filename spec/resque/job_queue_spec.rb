require 'spec_helper'

describe Resque::JobQueue do

  it "is Resque compatible" do # little joke
    Resque::JobQueue.should be_resque_compatible
  end

  it "should reserve a job" do
    a_job = Object.new
    ::Resque::Job.should_receive(:reserve).with(a_job).once

    Resque::JobQueue.reserve(a_job)
  end

  it "should list queues" do
    queues = %w{ a c b }
    ::Resque.should_receive(:queues).and_return( queues )

    Resque::JobQueue.list_queues.should == queues.sort
  end

end