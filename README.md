Resque DAG Queue plugin
=======================

A [Resque][rq] plugin for processing an acyclic directed graph of jobs.

Known to work with Resque 1.1.17 on Ubuntu 10.04 LTS. The maintainer will gladly
update this documentation with reports from other users of success or failure in
other environments.

This plugin allows one to enqueue a set of jobs which have interdepencies.
Workers remove items from the queue such that:

 (a) no job is worked before its dependencies are completed  WHILE ALSO

 (b) presenting the greatest number of jobs to workers for parallel processing

Sets of jobs are uniquely named and resque-web displays progress on the set.

Cyclic dependencies are detected and rejected.

The system also provides an exclusive locking mechanism for guaranteeing that
one and only one job which requires access to a specific external resource is
running at once. Jobs waiting on external resources do not consume available
workers.

If workers are waiting on dependencies to be met for one set, they begin work
on the next.

If a job fails, all jobs which depended on the failing job are skipped and the
set is permitted to complete, though marked as having failed.

All set meta-data stored in Redis may easily be disposed of with an expiry to
free memory.

Example usage:

    require 'dagqueue'

    # Add jobs to a queue
    class MyJob
      extend Dagqueue::Job

      def self.perform(*args)
        heavy_lifting
      end
    end

    lot = Dagqueue::Lot.new('a-queue-name')
    job_a, job_b, job_c =  MyJob.new, MyJob.new, MyJob.new

    lot.job( job_b ).requires( job_a ) # A silly example.
    lot.job( job_c ).requires( job_a ) # Dagqueue can handle thousands of
    lot.job( job_b ).requires( job_c ) # complex dependency relationships.

    Dagqueue.enqueue( lot )

    # Worker
    worker = Dagqueue::Worker.new('a-queue-name')
    worker.very_verbose = true
    worker.work 5

[rq]: http://github.com/defunkt/resque
