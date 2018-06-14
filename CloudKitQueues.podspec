Pod::Spec.new do |s|
  s.name         = "CloudKitQueues"
  s.version      = "0.0.1"
  s.summary      = "A queue manager for simplifying large batch operations on CloudKit records"
  s.homepage     = "https://github.com/sobri909/CloudKitQueues"
  s.author       = { "Matt Greenfield" => "matt@bigpaua.com" }
  s.license      = { :type => "MIT", :file => "LICENSE" }
  s.source       = { :git => 'https://github.com/sobri909/CloudKitQueues.git', :tag => '0.0.1' }
  s.source_files = 'Source/*.swift'
  s.ios.deployment_target = '11.0'
  s.frameworks   = 'CloudKit' 
end
