# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "jekyll-docs-theme"
  spec.version       = "0.1.1"
  spec.authors       = ["Vladimir 'allejo' Jimenez"]
  spec.email         = ["allejo@me.com"]

  spec.summary       = "A Jekyll Gem-based Theme for hosting documentation style websites"
  spec.homepage      = "https://github.com/allejo/jekyll-docs-theme"
  spec.license       = "MIT"

  spec.metadata["plugin_type"] = "theme"

  spec.files         = `git ls-files -z`.split("\x0").select do |f|
    f.match(%r{^(assets|docs|_(includes|layouts|sass)/|(LICENSE|README)((\.(txt|md|markdown)|$)))}i)
  end

  spec.add_runtime_dependency "jekyll", ">= 3.3", "< 5.0"

  # Updated to fix CVE-2016-7954 (Critical): Arbitrary code injection via secondary Gem source (fixed in Bundler 2.0+)
  # Updated to fix CVE-2021-43809 (Moderate): Local Code Execution via dash-leading git URL (fixed in Bundler 2.2.33+)
  spec.add_development_dependency "bundler", ">= 2.3.0"
  spec.add_development_dependency "rake", "~> 13.3"
end
