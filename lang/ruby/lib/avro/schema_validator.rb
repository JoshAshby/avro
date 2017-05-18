# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Avro
  class SchemaValidator
    class ValidationError < StandardError
      attr_reader :result

      def initialize(result)
        @result = result
        super
      end

      def to_s
        result.to_s
      end
    end

    class TypeMismatchError < ValidationError; end

    ROOT_IDENTIFIER = '.'.freeze
    PATH_SEPARATOR = '.'.freeze
    INT_RANGE = (Schema::INT_MIN_VALUE..Schema::INT_MAX_VALUE).freeze
    LONG_RANGE = (Schema::LONG_MIN_VALUE..Schema::LONG_MAX_VALUE).freeze
    COMPLEX_TYPES = [:array, :error, :map, :record, :request].freeze

    def self.validate *args
      new(*args).validate
    end

    def self.validate! *args
      new(*args).validate!
    end

    def initialize expected_schema, datum, root_identifier: ROOT_IDENTIFIER
      @expected_schema = expected_schema
      @datum           = datum
      @root_identifier = root_identifier
    end

    def path_errors
      @path_errors ||= Hash.new{ |hash, key| hash[ key ] = [] }
    end

    def errors
      path_errors.flat_map do |key, values|
        values.map do |value|
          "at #{ key } #{ value }"
        end
      end
    end

    def add_error(path, message)
      path_errors[path] << message
    end

    def failure?
      path_errors.any?
    end

    def successful?
      path_errors.empty?
    end

    def to_s
      errors.join("\n")
    end

    def validate
      validate_recursive(@expected_schema, @datum, root_identifier)
      self
    end

    def validate!
      return self if validate.successful?
      fail ValidationError, self
    end

    private

    attr_reader :root_identifier

    def merge_errors(other_errors)
      other_errors.each do |key, values|
        path_errors[key].concat(values)
      end
    end

    def validate_recursive(expected_schema, datum, path)
      catch(:type_error) do |tag|
        case expected_schema.type_sym
        when :null
          throw tag unless datum.nil?

        when :boolean
          throw tag unless [true, false].include?(datum)

        when :string, :bytes
          throw tag unless datum.is_a?(String)

        when :int
          throw tag unless datum.is_a?(Integer)

          return if INT_RANGE.cover?(datum)
          add_error(path, "out of bound value #{ datum }")

        when :long
          throw tag unless datum.is_a?(Integer)

          return if LONG_RANGE.cover?(datum)
          add_error(path, "out of bound value #{ datum }")

        when :float, :double
          throw tag unless [Float, Integer].any?(&datum.method(:is_a?))

        when :fixed
          if datum.is_a? String
            return if datum.bytesize == expected_schema.size

            message = "expected fixed with size #{ expected_schema.size }, got \"#{ datum }\" with size #{ datum.size }"
            add_error(path, message)
          else
            message = "expected fixed with size #{ expected_schema.size }, got #{ actual_value_message(datum) }"
            add_error(path, message)
          end

        when :enum
          return if expected_schema.symbols.include?(datum)

          message = "expected enum with values #{ expected_schema.symbols }, got #{ actual_value_message(datum) }"
          add_error(path, message)

        when :array
          throw tag unless datum.is_a?(Array)

          validate_array(expected_schema, datum, path)

        when :map
          throw tag unless datum.is_a?(Hash)

          validate_map(expected_schema, datum, path)

        when :union
          validate_union(expected_schema, datum, path)

        when :record, :error, :request
          throw tag unless datum.is_a?(Hash)

          validate_hash(expected_schema, datum, path)

        else
          fail "Unexpected schema type #{ expected_schema.type_sym } #{ expected_schema.inspect }"
        end

        return
      end

      add_error(path, "expected type #{ expected_schema.type_sym }, got #{ actual_value_message(datum) }")
    end

    def validate_hash(expected_schema, datum, path)
      expected_schema.fields.each do |field|
        deeper_path = deeper_path_for_hash(field.name, path)
        validate_recursive(field.type, datum[field.name], deeper_path)
      end
    end

    def validate_array(expected_schema, datum, path)
      datum.each_with_index do |d, i|
        validate_recursive(expected_schema.items, d, path + "[#{ i }]")
      end
    end

    def validate_map(expected_schema, datum, path)
      datum.keys.each do |key|
        next if key.is_a?(String)
        add_error(path, "unexpected key type '#{ ruby_to_avro_type(key.class) }' in map")
      end

      datum.each do |key, value|
        deeper_path = deeper_path_for_hash(key, path)
        validate_recursive(expected_schema.values, value, deeper_path)
      end
    end

    def validate_union(expected_schema, datum, path)
      if expected_schema.schemas.size == 1
        validate_recursive(expected_schema.schemas.first, datum, path)
        return
      end

      results = validate_possible_types(datum, expected_schema, path)
      successes, failures = results.partition { |r| r[:result].successful? }

      return if successes.any?

      complex_type_failed = failures.find { |r| COMPLEX_TYPES.include?(r[:type]) }

      if complex_type_failed
        merge_errors complex_type_failed[:result].path_errors
      else
        types = expected_schema.schemas.map { |s| "'#{ s.type_sym }'" }.join(', ')
        add_error(path, "expected union of [#{ types }], got #{ actual_value_message(datum) }")
      end
    end

    def validate_possible_types(datum, expected_schema, path)
      expected_schema.schemas.map do |schema|
        {
          type: schema.type_sym,
          result: self.class.validate(schema, datum, root_identifier: path)
        }
      end
    end

    def deeper_path_for_hash(*args)
      args.reverse.join(PATH_SEPARATOR).squeeze(PATH_SEPARATOR)
    end

    def actual_value_message(value)
      avro_type = if value.class == Integer
                    ruby_integer_to_avro_type(value)
                  else
                    ruby_to_avro_type(value.class)
                  end

      return avro_type if value.nil?

      "#{ avro_type } with value #{ value.inspect }"
    end

    def ruby_to_avro_type(ruby_class)
      {
        NilClass => 'null',
        String => 'string',
        Fixnum => 'int',
        Bignum => 'long',
        Float => 'float',
        Hash => 'record'
      }.fetch(ruby_class, ruby_class)
    end

    def ruby_integer_to_avro_type(value)
      INT_RANGE.cover?(value) ? 'int' : 'long'
    end
  end
end
