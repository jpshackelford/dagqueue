module Resque
  class JobQueue

    class << self

      def resque_compatible?
        true
      end

      def reserve(queue)
        ::Resque::Job.reserve(queue)
      end

      def list_queues
        ::Resque.queues.sort
      end

    end

  end
end