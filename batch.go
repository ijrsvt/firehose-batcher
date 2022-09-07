package firehosebatcher

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrBatchSizeOverflow   = errors.New("batch size overflow")
	ErrBatchLengthOverflow = errors.New("batch length overflow")
)

// Batch is really just a wrapper around a slice of `firehose.Record`s that tracks the size and length to make sure we don't create a batch that can't be sent to Firehose.
type Batch struct {
	size     int
	contents []types.Record

	fillTimer *prometheus.Timer
}

// NewBatch construct a batch with an intializing record
func NewBatch(r types.Record) *Batch {
	b := &Batch{
		fillTimer: prometheus.NewTimer(BatchFillLatency),
	}
	b.Add(r, false)

	BatchesCreated.Inc()

	return b
}

func (b *Batch) appendToLastRecord(r types.Record) bool {
	if b.Length() == 0 {
		return false
	}
	existingData := b.contents[b.Length()-1].Data
	if len(existingData)+len(r.Data)+1 >= PER_ITEM_SIZE_LIMIT {
		return false
	}
	if len(existingData) != 0 {
		existingData = append(existingData, []byte("\n")...)
	}
	b.contents[b.Length()-1].Data = append(existingData, r.Data...)

	return true

}

// Add attempts to add a record to the batch. If adding the record would cause either the batch's total size or total length to exceed AWS API limits this will return an appropriate error.
func (b *Batch) Add(r types.Record, packRecords bool) error {
	if b.contents == nil {
		b.contents = make([]types.Record, 0, BATCH_ITEM_LIMIT)
	}

	rSize := len(r.Data)
	if b.size+rSize > BATCH_SIZE_LIMIT {
		return ErrBatchSizeOverflow
	}

	if !(packRecords && b.appendToLastRecord(r)) {
		if b.Length()+1 > BATCH_ITEM_LIMIT {
			return ErrBatchLengthOverflow
		}

		b.contents = append(b.contents, r)
	}

	b.size += rSize
	BytesBatched.Add(float64(rSize))

	return nil
}

// Size return the number bytes stored in this batch
func (b *Batch) Size() int {
	return b.size
}

// Length return the number of records in the batch
func (b *Batch) Length() int {
	return len(b.contents)
}

// Send calls firehose.PutRecordBatch with the given batch. It does not current handle or retry on any sort of failure. This can cause unrecoverable message drops.
func (b *Batch) Send(client *firehose.Client, streamName string) error {
	prbo, err := client.PutRecordBatch(
		context.TODO(),
		&firehose.PutRecordBatchInput{
			DeliveryStreamName: &streamName,

			Records: b.contents,
		})
	if err != nil {
		return err
	}

	if *prbo.FailedPutCount > 0 {
		return errors.Errorf(
			"failed to send the full batch (%d failed), retries not currently handled",
			*prbo.FailedPutCount,
		)
	}

	return nil
}
