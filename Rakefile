unless defined?(Bundler)
  begin
    require 'bundler'
    Bundler.require(:development)
  rescue LoadError => e
    puts e
    puts <<-EOM
      Execute the following and retry:

      gem install bundler
      bundle install
    EOM

  end
end

if Bundler::VERSION == '1.0.11'
  puts "Known issue with Bundler 1.0.11 and Rake 0.9.\n" \
       "Please upgrade."
end

Bundler::GemHelper.install_tasks

# load rakefile extensions (tasks)

Dir['tasks/*.rake'].sort.each { |f| load f }

task :default => :spec
