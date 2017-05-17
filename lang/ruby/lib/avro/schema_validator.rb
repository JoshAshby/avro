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
    INT_RANGE = Schema::INT_MIN_VALUE..Schema::INT_MAX_VALUE
    LONG_RANGE = Schema::LONG_MIN_VALUE..Schema::LONG_MAX_VALUE
    COMPLEX_TYPES = [:array, :error, :map, :record, :request]

    def self.validate *args
      new(*args).validate
    end

    def self.validate! *args
      new(*args).validate!
    end

    attr_reader :root_identifier

    def initialize expected_schema, datum, root_identifier: ROOT_IDENTIFIER
      @expected_schema  = expected_schema
      @datum            = datum
      @root_identifier = root_identifier
    end

    def full_errors
      @full_errors ||= Hash.new{ |hash, key| hash[ key ] = [] }
    end

    def errors
      full_errors.flat_map do |key, values|
        values.map do |value|
          "at #{ key } #{ value }"
        end
      end
    end

    def add_error(path, message)
      full_errors[path] << message
    end

    def failure?
      full_errors.any?
    end

    def successful?
      full_errors.empty?
    end

    def merge_errors other_errors
      other_errors.each do |key, values|
        full_errors[ key ].concat values
      end
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

    def validate_recursive(expected_schema, datum, path)
      type_mismatch = :type_mismatch

      res = catch(:halt) do |tag|
        case expected_schema.type_sym
        when :null
          throw tag, type_mismatch unless datum.nil?

        when :boolean
          throw tag, type_mismatch unless [true, false].include?(datum)

        when :string, :bytes
          throw tag, type_mismatch unless datum.is_a?(String)

        when :int
          throw tag, type_mismatch unless datum.is_a?(Integer)

          add_error(path, "out of bound value #{datum}") unless INT_RANGE.cover?(datum)
          return

        when :long
          throw tag, type_mismatch unless datum.is_a?(Integer)

          add_error(path, "out of bound value #{datum}") unless LONG_RANGE.cover?(datum)
          return

        when :float, :double
          throw tag, type_mismatch unless [Float, Integer].any?(&datum.method(:is_a?))

        when :fixed
          if datum.is_a? String
            return if datum.bytesize == expected_schema.size

            message = "expected fixed with size #{expected_schema.size}, got \"#{datum}\" with size #{datum.size}"
            add_error(path, message)
          else
            message = "expected fixed with size #{expected_schema.size}, got #{actual_value_message(datum)}"
            add_error(path, message)
          end

          return

        when :enum
          return if expected_schema.symbols.include?(datum)

          message = "expected enum with values #{expected_schema.symbols}, got #{actual_value_message(datum)}"
          add_error(path, message)
          return

        when :array
          return validate_array(expected_schema, datum, path)

        when :map
          return validate_map(expected_schema, datum, path)

        when :union
          return validate_union(expected_schema, datum, path)

        when :record, :error, :request
          return validate_hash(expected_schema, datum, path)

        else
          fail "Unexpected schema type #{expected_schema.type_sym} #{expected_schema.inspect}"
        end
      end

      if res == type_mismatch
        add_error(path, "expected type #{expected_schema.type_sym}, got #{actual_value_message(datum)}")
      end
    end

    def validate_hash(expected_schema, datum, path)
      unless datum.is_a?(Hash)
        add_error(path, "expected type #{expected_schema.type_sym}, got #{actual_value_message(datum)}")
        return
      end

      expected_schema.fields.each do |field|
        deeper_path = deeper_path_for_hash(field.name, path)
        validate_recursive(field.type, datum[field.name], deeper_path)
      end
    end

    def validate_array(expected_schema, datum, path)
      unless datum.is_a?(Array)
        add_error(path, "expected type #{expected_schema.type_sym}, got #{actual_value_message(datum)}")
        return
      end

      datum.each_with_index do |d, i|
        validate_recursive(expected_schema.items, d, path + "[#{i}]")
      end
    end

    def validate_map(expected_schema, datum, path)
      datum.keys.each do |k|
        next if k.is_a?(String)
        add_error(path, "unexpected key type '#{ruby_to_avro_type(k.class)}' in map")
      end

      datum.each do |k, v|
        deeper_path = deeper_path_for_hash(k, path)
        validate_recursive(expected_schema.values, v, deeper_path)
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
        merge_errors complex_type_failed[:result].full_errors
      else
        types = expected_schema.schemas.map { |s| "'#{s.type_sym}'" }.join(', ')
        add_error(path, "expected union of [#{types}], got #{actual_value_message(datum)}")
      end
    end

    def validate_possible_types(datum, expected_schema, path)
      expected_schema.schemas.map do |schema|
        { type: schema.type_sym, result: self.class.validate(schema, datum, root_identifier: path) }
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

      if value.nil?
        avro_type
      else
        "#{avro_type} with value #{value.inspect}"
      end
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
