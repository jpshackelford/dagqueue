# Thanks to mattetti@github (Matt Aimonetti)
# https://github.com/mattetti/GC-stats-middleware/blob/master/gc_stats.rb

module RubyGC
  class << self

    def prof(n = 1)
      puts "PID: #{Process.pid}"
      sleep 5
      # GC::Profiler.enable unless GC::Profiler.enabled?
      before_stats = ObjectSpace.count_objects
      t            = start_display_thread(before_stats)
      begin
        1.upto(n) { yield }
      ensure
        t[:stop] = true
        t.join
        after_stats = ObjectSpace.count_objects
        final_display(n, before_stats, after_stats)
      end
    end

    def start_display_thread(first_run)
      Thread.abort_on_exception = true
      Thread.start(first_run) do |first_run|
        last_run = first_run
        record_highs_and_lows(last_run)
        loop do
          break if Thread.current[:stop]
          print '.'
          sleep 0.5
          new_run = ObjectSpace.count_objects
          record_highs_and_lows(new_run)
          # short_display(last_run, new_run)
          last_run = new_run
          print 'o'
        end
      end
    end

    def record_highs_and_lows(object_count)
      @highs ||= { }
      @lows  ||= { }

      @highs.update(object_count) do |k, v1, v2|
        v1 >= v2 ? v1 : v2
      end
      @lows.update(object_count) do |k, v1, v2|
        v1 <= v2 ? v1 : v2
      end
    end

    def final_display(n, before_stats, after_stats)
      puts '-' * 40
      puts "GC activity after #{n} runs:"
      long_display(before_stats, after_stats)
    end

    def short_display(before_stats, after_stats)
      print "objs: #{after_stats[:TOTAL]}\t"
      report = GC::Profiler.result
      GC::Profiler.clear
      if report != ''
        unless defined?(@headings_displayed)
          puts report.split("\n")[1]
          @headings_displayed = true
        else
          puts report.split("\n").last
        end
      end
    end

    def long_display(before_stats, after_stats)

      if before_stats[:TOTAL] < after_stats[:TOTAL]
        puts "Total objects in memory bumped by #{after_stats[:TOTAL] - before_stats[:TOTAL]} objects."
      end

      if before_stats[:FREE] > after_stats[:FREE]
        puts "\033[0;32m[GC Stats]\033[1;33m #{before_stats[:FREE] - after_stats[:FREE]} new allocated objects.\033[0m"
      else
        sleep 0.5
        # report = GC::Profiler.result
        # GC::Profiler.clear
        # puts report
        puts "%-10s %10s %10s %10s %10s" % %w[Stat Initial Low High Current]
        @highs.keys.sort.each do |k|
          puts "%-10s %10d %10d %10d %10d" % [k, before_stats[k], @lows[k], @highs[k], after_stats[k]]
        end
      end

    end

    def red(text)
      "\033[0;31m#{text}\033[0m"
    end

  end
end

task 'gcprof' do

  class A
    def initialize(o=nil)
      @s = 'happy' + rand.to_s
      @o = o
    end
  end

  class B
    def initialize
      @a = []
    end

    def add
      @a << A.new(self)
    end
  end

  RubyGC.prof(1) do
    c = []
    1.upto(10000) do
      c << b = B.new
      print '+'
      1.upto(10000) { b.add }
      c.shift if c.size > 2
    end
  end

end
