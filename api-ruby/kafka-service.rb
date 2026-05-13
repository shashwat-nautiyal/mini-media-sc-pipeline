require 'kafka'
require 'json'

class KafkaService
  def initialize
    @kafka = Kafka.new([ENV['KAFKA_BROKERS']], client_id: "ruby-api-ingress")
    @producer = @kafka.producer
    @topic = ENV['KAFKA_TOPIC']
  end

  def publish_upload_event(file_id, s3_uri)
    payload = {
      file_id: file_id,
      s3_uri: s3_uri,
      timestamp: Time.now.utc.iso8601,
      status: 'UPLOADED'
    }.to_json

    # required_acks: :all guarantees the event is written to all sync replicas
    @producer.produce(payload, topic: @topic, partition_key: file_id)
    @producer.deliver_messages
  rescue Kafka::Error => e
    # In production, implement a fallback mechanism (e.g., outbox pattern) here
    raise "Failed to publish to Kafka: #{e.message}"
  end
end