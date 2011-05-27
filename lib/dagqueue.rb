require 'resque'

module Resque
  module Plugins
    module Dagque

      extend Resque
      extend self

      def enqueue
        Resque.enqueue
      end


    end
  end
end