# frozen_string_literal: true

module Sass
  # The {ForkTracker} module.
  #
  # It tracks objects that need to be closed after `Process.fork`.
  module ForkTracker
    module_function

    if Process.respond_to?(:_fork)
      # TODO: remove next line once ruby 3.1 support is dropped
      require 'set' unless defined?(::Set)

      SET = Set.new.compare_by_identity

      MUTEX = Mutex.new

      private_constant :SET, :MUTEX

      def add(object)
        MUTEX.synchronize do
          SET.add(object)
        end
      end

      def delete(object)
        MUTEX.synchronize do
          SET.delete(object)
        end
      end

      def each(&)
        MUTEX.synchronize do
          SET.to_a
        end.each(&)
      end

      # The {CoreExt} module.
      #
      # It closes objects after `Process.fork`.
      module CoreExt
        def _fork
          pid = super
          ForkTracker.each(&:close) if pid.zero?
          pid
        end
      end

      private_constant :CoreExt

      Process.singleton_class.prepend(CoreExt)
    else
      def add(object); end
      def delete(object); end
    end
  end

  private_constant :ForkTracker
end
