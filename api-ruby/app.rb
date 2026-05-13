require 'sinatra'
require 'dotenv/load'
require 'securerandom'
require_relative 's3_service'
require_relative 'kafka_service'

set :bind, '0.0.0.0'
set :port, ENV['PORT']
set :server, :puma

# Initialize singletons for connection pooling
s3_client = S3Service.new
kafka_client = KafkaService.new

post '/upload' do
  unless params[:file] && params[:file][:tempfile]
    halt 400, { error: 'No file provided' }.to_json
  end

  file = params[:file][:tempfile]
  filename = params[:file][:filename]
  extension = File.extname(filename)
  
  # Generate idempotent identifier
  file_id = SecureRandom.uuid

  begin
    # 1. Persist to Storage
    s3_uri = s3_client.upload(file.path, file_id, extension)
    
    # 2. Emit Event
    kafka_client.publish_upload_event(file_id, s3_uri)

    status 202
    content_type :json
    {
      message: 'Upload accepted and queued for processing',
      file_id: file_id,
      s3_uri: s3_uri
    }.to_json
  rescue StandardError => e
    halt 500, { error: 'Internal Server Error', details: e.message }.to_json
  end
end