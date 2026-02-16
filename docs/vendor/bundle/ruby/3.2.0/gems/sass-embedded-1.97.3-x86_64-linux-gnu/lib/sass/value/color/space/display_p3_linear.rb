# frozen_string_literal: true

module Sass
  module Value
    class Color
      module Space
        # @see https://github.com/sass/dart-sass/blob/main/lib/src/value/color/space/display_p3_linear.dart
        class DisplayP3Linear
          include Space

          def bounded?
            true
          end

          def initialize
            super('display-p3-linear', Utils::RGB_CHANNELS)
          end

          def convert(dest, red, green, blue, alpha)
            if dest == DISPLAY_P3
              Color.send(
                :for_space_internal,
                dest,
                red.nil? ? nil : Utils.srgb_and_display_p3_from_linear(red),
                green.nil? ? nil : Utils.srgb_and_display_p3_from_linear(green),
                blue.nil? ? nil : Utils.srgb_and_display_p3_from_linear(blue),
                alpha
              )
            else
              super
            end
          end

          def to_linear(channel)
            channel
          end

          def from_linear(channel)
            channel
          end

          private

          def transformation_matrix(dest)
            case dest
            when A98_RGB
              Conversions::LINEAR_DISPLAY_P3_TO_LINEAR_A98_RGB
            when LMS
              Conversions::LINEAR_DISPLAY_P3_TO_LMS
            when PROPHOTO_RGB
              Conversions::LINEAR_DISPLAY_P3_TO_LINEAR_PROPHOTO_RGB
            when REC2020
              Conversions::LINEAR_DISPLAY_P3_TO_LINEAR_REC2020
            when RGB, SRGB, SRGB_LINEAR
              Conversions::LINEAR_DISPLAY_P3_TO_LINEAR_SRGB
            when XYZ_D50
              Conversions::LINEAR_DISPLAY_P3_TO_XYZ_D50
            when XYZ_D65
              Conversions::LINEAR_DISPLAY_P3_TO_XYZ_D65
            else
              super
            end
          end
        end

        private_constant :DisplayP3Linear

        DISPLAY_P3_LINEAR = DisplayP3Linear.new
      end
    end
  end
end
