# encoding: utf-8

require 'bundler/setup'
require 'bundler/gem_helper'
require 'rspec/core/rake_task'

namespace :extension do
  task :package do
    Dir.chdir('ext') do
      sh "mvn -Djruby-version=#{JRUBY_VERSION} package"
    end
  end

  task :install => :package do
    extension_jar = Dir['ext/target/rocksdb-jruby-*-jar-with-dependencies.jar'].max
    extension_dir = 'lib/rocks_db/ext'
    FileUtils.mkdir_p(extension_dir)
    FileUtils.cp(extension_jar, File.join(extension_dir, 'rocks_db.jar'))
  end
end

namespace :bundler do
  Bundler::GemHelper.install_tasks
end

task :release => [:spec, 'extension:install', 'bundler:release']

desc 'Run specs'
RSpec::Core::RakeTask.new(:spec)

task :spec => 'extension:install'

task :default => :spec
