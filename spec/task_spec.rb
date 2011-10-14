require 'spec_helper'

include Dagqueue

describe Dagqueue::Task do

  it "it may require other jobs to be run first" do
    job1 = Task.new
    job2 = Task.new
    job3 = Task.new
    job1.requires(job2, job3)
    job1.requirements.sort.should == [job2, job3].sort
  end

  it "requirements may be specified in an array, with duplicates" do
    job1 = Task.new
    job2 = Task.new
    job3 = Task.new
    job1.requires([job2, job3], job2)
    job1.requirements.sort.should == [job2, job3].sort
  end

  it "raises an error if something other than a job is specified as a requirement" do
    job1 = Task.new
    expect { job1.requires(Object.new) }.to raise_error(ArgumentError)
  end

  it "does not consider different instances of a job equal" do
    job1 = Task.new
    job2 = Task.new
    job1.should_not == job2
  end

  it "does consider the same instance of a job equal" do
    job1 = Task.new
    job1.should == job1
  end

  it "silently ignores self-referential requirements" do
    job1 = Task.new
    job1.requires(job1)
    job1.requirements.should be_empty
  end

  it "obtains a unique id from redis" do
    Task.new.unique_id.should_not == Task.new.unique_id
  end

  it "keeps the same id for the life of the object" do
    job1 = Task.new
    job1.unique_id.should == job1.unique_id
  end

end