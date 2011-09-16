# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "dagqueue"
  s.version     = '0.0.1'
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["John-Mason P. Shackelford"]
  s.email       = ["jpshack@gmail.com"]
  s.homepage    = "http://github.com/jpshackelford/dagqueue"
  s.summary     = %q{Resque plugin to handle processing a directed acyclic graph of jobs}

  s.description = <<EOM

  
EOM

  s.rubyforge_project = "dagqueue"

  s.files             = %w( README.md LICENSE HISTORY.md )
  s.files            += Dir.glob("lib/**/*")
  s.files            += Dir.glob("spec/**/*")
  s.files            += Dir.glob("tasks/**/*")

  s.require_paths = ["lib"]

  #  s.files            += Dir.glob("bin/**/*")
  #  s.executables       = [ "resque, "resque-web" ]

  s.test_files    = Dir.glob("spec/**/*")

  s.extra_rdoc_files  = [ "LICENSE", "README.md", "HISTORY.md" ]
  s.rdoc_options      = ["--charset=UTF-8"]

  s.add_dependency 'resque',        '~> 1.17.1'
  s.add_dependency 'redis',         '~> 2.2.0'
  s.add_dependency 'redis-objects', '~> 0.5.1'
  s.add_dependency 'rgl',           '~> 0.4.0'

end
