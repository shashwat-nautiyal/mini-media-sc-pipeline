// Workflow dictates the sequence, retry policies, and timeout
// constraints of the activities. Workflow code must be strictly deterministic.
package main

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// MediaProcessingWorkflow orchestrates the media supply chain.
func MediaProcessingWorkflow(ctx workflow.Context, event UploadEvent) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Media processing workflow started", "FileID", event.FileID)

	// Configure Activity Execution characteristics
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 1,
			MaximumAttempts:    3, // Fail the workflow after 3 activity failures
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Extract Metadata
	var metadataResult string
	err := workflow.ExecuteActivity(ctx, ExtractMetadataActivity, event.S3URI).Get(ctx, &metadataResult)
	if err != nil {
		logger.Error("Metadata extraction failed", "Error", err)
		return "", err
	}

	// Step 2: Transcode
	var transcodeResult string
	err = workflow.ExecuteActivity(ctx, TranscodeActivity, event.S3URI).Get(ctx, &transcodeResult)
	if err != nil {
		logger.Error("Transcode failed", "Error", err)
		return "", err
	}

	// Step 3: Quality Control
	var qcResult bool
	err = workflow.ExecuteActivity(ctx, QCActivity, transcodeResult).Get(ctx, &qcResult)
	if err != nil {
		logger.Error("QC failed", "Error", err)
		return "", err
	}

	if !qcResult {
		return "", temporal.NewNonRetryableApplicationError("QC Failed: Artifact rejected", "QC_ERROR", nil)
	}

	logger.Info("Media processing workflow completed successfully", "FinalArtifact", transcodeResult)
	return transcodeResult, nil
}