package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type UploadEvent struct {
	FileID    string `json:"file_id"`
	S3URI     string `json:"s3_uri"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

func main() {
	temporalHost := os.Getenv("TEMPORAL_HOST_PORT")
	if temporalHost == "" {
		temporalHost = "localhost:7233"
	}

	c, err := client.Dial(client.Options{
		HostPort: temporalHost,
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client", err)
	}
	defer c.Close()

	// --- NEW: Start Temporal Worker ---
	taskQueue := "MEDIA_PROCESSING_TASK_QUEUE"
	w := worker.New(c, taskQueue, worker.Options{})

	w.RegisterWorkflow(MediaProcessingWorkflow)
	w.RegisterActivity(ExtractMetadataActivity)
	w.RegisterActivity(TranscodeActivity)
	w.RegisterActivity(QCActivity)

	// Start the worker in a non-blocking goroutine
	err = w.Start()
	if err != nil {
		log.Fatalln("Unable to start Temporal worker", err)
	}
	defer w.Stop()
	log.Printf("Temporal Worker started on queue: %s", taskQueue)
	// ----------------------------------

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		GroupID:  "go-temporal-bridge-group",
		Topic:    "media.uploaded",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()
	log.Println("Listening for events on Kafka topic: media.uploaded")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka read error: %v", err)
			continue
		}

		var event UploadEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Failed to unmarshal JSON: %v", err)
			continue
		}

		log.Printf("Consumed Kafka Event -> FileID: %s", event.FileID)

		options := client.StartWorkflowOptions{
			ID:        "media-pipeline-" + event.FileID,
			TaskQueue: taskQueue,
		}

		we, err := c.ExecuteWorkflow(context.Background(), options, MediaProcessingWorkflow, event)
		if err != nil {
			log.Printf("Unable to execute workflow: %v", err)
		} else {
			log.Printf("Triggered Workflow -> ID: %s", we.GetID())
		}
	}
}