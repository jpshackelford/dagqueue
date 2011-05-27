require 'spec_helper'

include Resque::Plugins

describe 'Dagque' do
  
  it "has an enqueue method which delegates to Resque" do
    Resque.should_receive(:enqueue)

    Dagque.should respond_to(:enqueue)
    Dagque.enqueue
  end

end