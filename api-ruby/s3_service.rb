require 'aws-sdk-s3'

class S3Service
  def initialize
    @client = Aws::S3::Client.new(
      endpoint: ENV['S3_ENDPOINT'],
      region: ENV['AWS_REGION'],
      force_path_style: true, # Required for LocalStack
      access_key_id: ENV['AWS_ACCESS_KEY_ID'],
      secret_access_key: ENV['AWS_SECRET_ACCESS_KEY']
    )
    @bucket = ENV['S3_BUCKET']
  end

  def upload(file_path, file_id, extension)
    s3_key = "inbox/#{file_id}#{extension}"
    
    @client.put_object(
      bucket: @bucket,
      key: s3_key,
      body: File.open(file_path, 'rb')
    )
    
    "s3://#{@bucket}/#{s3_key}"
  end
end