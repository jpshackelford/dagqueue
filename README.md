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

    dag = Dagqueue::Dag.new('dag-name', 'a-queue-name')
    job_a, job_b, job_c =  MyJob.new, MyJob.new, MyJob.new

    dag.job( job_b ).requires( job_a ) # A silly example.
    dag.job( job_c ).requires( job_a ) # Dagqueue can handle thousands of
    dag.job( job_b ).requires( job_c ) # complex dependency relationships.

    Dagqueue.enqueue( dag )

    # Worker
    worker = Dagqueue::Worker.new('a-queue-name')
    worker.very_verbose = true
    worker.work 5

    # Results
    dag = Dag.find('dag-name')
    dag.percent_complete
    dag.jobs( :successful ) # :waiting, :failed, :skipped, :completed,
                            # :in_progress, :queued, :waiting-resource,
                            # :waiting-dependency
    dag.failed?
    dag.succeeded?
    dag.start_time
    dag.stop_time
    dag.run_duration
    dag.size

    dag.abort!             # Mark all incomplete jobs (except those currently
                           # in the hands of workers, as skipped.)

    dag.expire_metadata!   # Ensure that all meta-data is removed from redis
                           # upon completion of dag jobs.

    dag.preserve_metadata! # Keep meta-data after dag is complete.

    dag.to_json            # Support for serializing dag results for
    dag.from_json( json )  # later use.

    job.host               # You can see where in-progress jobs
    job.pid                # are running!

    # Hooks for your use!
    dag.started!
                        job.queued!
    dag.waiting!        job.waiting!  # for an external resource
                        job.in_progress! # called just before perform
    dag.aborted!
                        job.skipped!
    dag.complete!       job.complete!
    dag.failed!         job.failed!
    dag.succeeded!      job.succeeded!

[rq]: http://github.com/defunkt/resque
