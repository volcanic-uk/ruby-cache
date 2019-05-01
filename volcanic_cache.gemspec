require_relative 'lib/volcanic/cache/version.rb'

Gem::Specification.new do | spec|
  spec.name = 'volcanic_cache'
  spec.version = Volcanic::Cache::VERSION
  spec.summary = 'A simple in-memory cache'
  spec.description = spec.summary
  spec.license = 'Nonstandard'
  spec.authors = ['d.leyden@volcanic.co.uk']
  spec.files = Dir.glob 'lib/**/*.rb'
  spec.require_paths = ['lib']

  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
  spec.add_development_dependency 'rspec-its', '~> 1.3'
  spec.add_development_dependency 'rubocop', '~> 0.57.2'
  spec.add_development_dependency 'bundler', '~> 1.16'
  spec.add_development_dependency 'pry'
end
