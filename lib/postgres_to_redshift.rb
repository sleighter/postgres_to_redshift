require "postgres_to_redshift/version"
require 'pg'
require 'uri'
require 'aws-sdk'
require 'zlib'
require 'stringio'
require "postgres_to_redshift/table"
require "postgres_to_redshift/column"

class PostgresToRedshift

  PART_SIZE = 100000

  class << self
    attr_accessor :source_uri, :target_uri
  end

  attr_reader :source_connection, :target_connection, :s3

  @errors = []
  def self.update_tables
    @start_time = Time.now
    puts "#{@start_time}Starting migration process with settings: \n" +
      "POSTGRES_TO_REDSHIFT_SOURCE_URI:  #{ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI']}\n" +
      "S3_DATABASE_EXPORT_ID:            #{ENV['S3_DATABASE_EXPORT_ID']}\n"           +
      "S3_DATABASE_EXPORT_KEY:           #{ENV['S3_DATABASE_EXPORT_KEY']}\n"          +
      "POSTGRES_TO_REDSHIFT_TARGET_URI:  #{ENV['POSTGRES_TO_REDSHIFT_TARGET_URI']}\n" +
      "S3_DATABASE_EXPORT_BUCKET:        #{ENV['S3_DATABASE_EXPORT_BUCKET']}\n"       +
      "S3_DATABASE_EXPORT_BUCKET_REGION: #{ENV['S3_DATABASE_EXPORT_BUCKET_REGION']}\n"
    update_tables = PostgresToRedshift.new
    tables_to_update = update_tables.tables

    tables_to_update.each_with_index do |table,index|
      puts "Processing Part #{index} of #{tables_to_update.count} [#{(index * 100.0 / tables_to_update.count).round(2)}%]" rescue nil
      update_tables.update_table(table)
    end
    @total_time = Time.now - @start_time
    puts ("Total run time: #{@total_time} seconds")
    puts "Suppressed Errors: "
    puts @errors.join('\n')
  end

  def self.source_uri
    @source_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI'])
  end

  def self.target_uri
    @target_uri ||= URI.parse(ENV['POSTGRES_TO_REDSHIFT_TARGET_URI'])
  end

  def self.source_connection
    unless instance_variable_defined?(:"@source_connection")
      @source_connection = PG::Connection.new(host: source_uri.host, port: source_uri.port, user: source_uri.user || ENV['USER'], password: source_uri.password, dbname: source_uri.path[1..-1])
      @source_connection.exec("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;")
    end

    @source_connection
  end

  def self.target_connection
    unless instance_variable_defined?(:"@target_connection")
      @target_connection = PG::Connection.new(host: target_uri.host, port: target_uri.port, user: target_uri.user || ENV['USER'], password: target_uri.password, dbname: target_uri.path[1..-1])
    end

    @target_connection
  end

  def self.drop_target_tables
    PostgresToRedshift.new.target_table_names.each do |table_name|
      puts "Dropping #{table_name} on target"
      target_connection.exec("DROP TABLE IF EXISTS #{table_name}")
    end
  end

  def update_table(table)
    puts "Creating Table: #{table.target_table_name}"
    target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{table.target_table_name} (#{table.columns_for_create})")

    puts "Copying Table #{table.target_table_name} Part #{table.table_part} from Postgres"
    copy_table(table)

    puts "Importing Table #{table.target_table_name} Part #{table.table_part} to Redshift"
    import_table(table)
  end

  def source_connection
    self.class.source_connection
  end

  def target_connection
    self.class.target_connection
  end

  def unique_tables
    @unique_tables ||= source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE')").map do |table_attributes|
      table = Table.new(attributes: table_attributes.merge(table_part: 1))
      next if table.name =~ /(^pg_|_sys$|_catalog$|^geography_columns$|^subscriptions$|^content$|^driver_locations$)/
      table.columns = column_definitions(table)
      table
    end.compact
  end

  def tables
    @tables ||= source_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE')").each_with_index.flat_map do |table_attributes,index|
      table = Table.new(attributes: table_attributes.merge(table_part: 1))
      next if table.name =~ /(^pg_|_sys$|_catalog$|^geography_columns$|^subscriptions$|^content$|^driver_locations$|^monthly_order_counts$)/
      rows = row_count(table)
      if rows > PART_SIZE
        table_parts = []
        count = 0
        part_count = 1
        while count < rows
          t = Table.new(attributes: table_attributes.merge(table_part: part_count))
          t.columns = column_definitions(t)
          table_parts << t
          count += PART_SIZE
          part_count += 1
          puts "[Table ##{index}] Found #{t.name} Part #{t.table_part}"
        end
        table_parts
      else
        puts "[Table ##{index}] Found #{table.name} Part #{table.table_part}"
        table.columns = column_definitions(table)
        table
      end
    end.compact
  end

  def target_table_names
    result = target_connection.exec("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_type in ('BASE TABLE')")
    result.map { |r| r['table_name'] }
  end

  def row_count(table)
    source_connection.exec("SELECT COUNT(*) FROM #{table.name}").getvalue(0,0).to_i
  end

  def column_definitions(table)
    source_connection.exec("SELECT * FROM information_schema.columns WHERE table_schema='public' AND table_name='#{table.name}' AND column_name NOT IN ('area_geometry') order by ordinal_position")
  end

  def s3
    @s3 ||= build_s3
  end

  def build_s3
    Aws.config.update({
      credentials: Aws::Credentials.new(ENV['S3_DATABASE_EXPORT_ID'], ENV['S3_DATABASE_EXPORT_KEY']),
    })
    Aws::S3::Resource.new(region: ENV['S3_DATABASE_EXPORT_BUCKET_REGION'])
  end

  def bucket
    @bucket ||= s3.bucket(ENV['S3_DATABASE_EXPORT_BUCKET'])
  end

  def dump_exists(table)
    bucket.object("export/#{table.target_table_name}_#{table.table_part}.psv.gz").content_length > 0 rescue false
  end

  def copy_table(table)
    unless dump_exists(table)
      puts "Writing to file #{table.target_table_name}_#{table.table_part}.psv.gz"
      File.open("#{table.target_table_name}_#{table.table_part}.psv.gz", "w+") do |buffer|
        zip = Zlib::GzipWriter.new(buffer)

        puts "Downloading #{table.name}"

        copy_command = "COPY (SELECT #{table.columns_for_copy} FROM #{table.name} ORDER BY #{table.order_by} LIMIT #{PART_SIZE} OFFSET #{(table.table_part - 1) * PART_SIZE}) TO STDOUT WITH DELIMITER '|'"

        source_connection.copy_data(copy_command) do
          while row = source_connection.get_copy_data
            zip.write(row)
          end
        end
        zip.finish
      end

      upload_table(table)

      File.delete("#{table.target_table_name}_#{table.table_part}.psv.gz")
    else
      puts "Dump of #{table.target_table_name} Part #{table.table_part} already exists. Skipping..."
    end
  end

  def upload_table(table)
    puts "Uploading #{table.target_table_name}_#{table.table_part}"
    obj = bucket.object("export/#{table.target_table_name}_#{table.table_part}.psv.gz")
    obj.upload_file("#{table.target_table_name}_#{table.table_part}.psv.gz")
  end

  def import_table(table)
    puts "Importing #{table.target_table_name} Part #{table.table_part}"

    begin
      target_connection.exec("BEGIN;")

      target_connection.exec("CREATE TABLE IF NOT EXISTS public.#{table.target_table_name} (#{table.columns_for_create})")

      target_connection.exec("COPY public.#{table.target_table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}_#{table.table_part}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE ACCEPTINVCHARS DELIMITER as '|'")

      target_connection.exec("COMMIT;")
    rescue
      @errors << "Exception occurred during import of #{table.target_table_name} Part #{table.table_part}"
    end
  end

  def puts(msg)
    super(msg)
    STDOUT.flush
  end
end
