package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

// ExtractMetadataActivity simulates downloading the file header from S3 and parsing codecs.
func ExtractMetadataActivity(ctx context.Context, s3URI string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Extracting metadata", "S3URI", s3URI)

	// Simulate network I/O and ffprobe execution
	time.Sleep(2 * time.Second)

	// In a real system, you would execute: `ffprobe -v quiet -print_format json -show_format -show_streams s3_presigned_url`
	metadata := fmt.Sprintf("Metadata extracted for %s: Codec=H.264, Resolution=1920x1080", s3URI)
	return metadata, nil
}

// TranscodeActivity simulates generating a 720p proxy video.
func TranscodeActivity(ctx context.Context, s3URI string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Starting transcode process", "S3URI", s3URI)

	// Simulate heavy CPU bound work
	// Temporal's heartbeat mechanism would normally be used here for long-running transcodes.
	time.Sleep(5 * time.Second)

	outputURI := s3URI + "_720p_proxy.mp4"
	logger.Info("Transcode complete", "OutputURI", outputURI)
	return outputURI, nil
}

// QCActivity simulates a quality control pass (e.g., checking for pure black frames or audio clipping).
func QCActivity(ctx context.Context, transcodeURI string) (bool, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Running Quality Control pass", "TargetURI", transcodeURI)

	time.Sleep(1 * time.Second)

	// Simulate a successful QC pass
	return true, nil
}