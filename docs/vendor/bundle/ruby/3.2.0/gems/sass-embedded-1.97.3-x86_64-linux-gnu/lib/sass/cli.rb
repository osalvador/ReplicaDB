# frozen_string_literal: true

require 'sass/elf'

module Sass
  module CLI
    INTERPRETER = '/lib64/ld-linux-x86-64.so.2'

    INTERPRETER_SUFFIX = '/ld-linux-x86-64.so.2'

    COMMAND = [
      *(ELF::INTERPRETER if ELF::INTERPRETER != INTERPRETER && ELF::INTERPRETER&.end_with?(INTERPRETER_SUFFIX)),
      File.absolute_path('dart-sass/src/dart', __dir__).freeze,
      File.absolute_path('dart-sass/src/sass.snapshot', __dir__).freeze
    ].freeze
  end

  private_constant :CLI
end
