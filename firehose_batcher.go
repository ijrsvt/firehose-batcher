package firehosebatcher

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// FirehoseBatcher is a wrapper around a firehose client that makes it easier to send data to firehose using the PutRecordBatch API. It'll buffer data internally to construct batches before sending them onward.
type FirehoseBatcher struct {
	streamName      string
	maxSendInterval time.Duration

	firehoseClient *firehose.Client

	inputBuffer     chan []byte
	batchSendBuffer chan *Batch
}

// New constructs a FirehoseBatcher that will send batches to Firehose whenever either a batch is full (size or length) or every interval.
func New(fc *firehose.Client, streamName string, sendTimeout time.Duration) (*FirehoseBatcher, error) {
	fb := &FirehoseBatcher{
		streamName:      streamName,
		maxSendInterval: sendTimeout,

		firehoseClient: fc,

		inputBuffer:     make(chan []byte, BATCH_ITEM_LIMIT),
		batchSendBuffer: make(chan *Batch), // TODO(@thomas): Consider making this an actual buffer when we can run multiple senders
	}

	return fb, nil
}

// AddRaw takes a byte buffer to send to Firehose. It will return an error if the size of msg exceeds the max allowed item size (see limits.go). Will block if the send buffers are full.
func (fb *FirehoseBatcher) AddRaw(msg []byte) error {
	if len(msg) > PER_ITEM_SIZE_LIMIT {
		return errors.New("item exceeds firehose's max item size")
	}

	fb.inputBuffer <- msg
	BytesRead.Add(float64(len(msg)))
	return nil
}

// AddRawFromChan is a convenience wrapper around Add that just keeps adding records until an error occurs.
func (fb *FirehoseBatcher) AddRawFromChan(c chan []byte) error {
	for msg := range c {
		if err := fb.AddRaw(msg); err != nil {
			return errors.Wrap(err, "failed to add record to batcher")
		}
	}

	return nil
}

func (fb *FirehoseBatcher) startBatching() {
	defer close(fb.batchSendBuffer)

	for {
		initial := <-fb.inputBuffer
		batch := NewBatch(types.Record{Data: initial})

	BatchingLoop:
		for batch.Length() < BATCH_ITEM_LIMIT {
			select {
			// Timeout, send even if not full
			case <-time.After(fb.maxSendInterval):
				break BatchingLoop

			// Read next and try to include in batch
			case b, ok := <-fb.inputBuffer:
				if !ok {
					// Input channel is closed, we're done here send the last batch and return
					fb.queueBatch(batch)
					return
				}

				record := types.Record{Data: b}
				switch err := batch.Add(record); err {
				case nil:
					// Noop
				case ErrBatchSizeOverflow, ErrBatchLengthOverflow:
					// Batch is full: send the batch along and start a new batch with the overflowing record
					fb.queueBatch(batch)
					batch = NewBatch(record)
				default:
					panic("Unknown error from batch construction")
				}
			}
		}

		fb.queueBatch(batch)
	}
}

func (fb *FirehoseBatcher) queueBatch(batch *Batch) {
	BatchSize.Observe(float64(batch.Size()))
	BatchLength.Observe(float64(batch.Length()))

	batch.fillTimer.ObserveDuration()

	fb.batchSendBuffer <- batch
}

// TODO(@thomas): retry logic here :D
func (fb *FirehoseBatcher) sendBatches(streamName string) error {
	for batch := range fb.batchSendBuffer {
		t := prometheus.NewTimer(BatchSendLatency)

		err := batch.Send(fb.firehoseClient, streamName)
		if err != nil {
			return errors.Wrap(err, "error sending batch")
		}

		t.ObserveDuration()
		BytesSent.Add(float64(batch.Size()))
		BatchesSent.Inc()
	}

	return nil
}

// Start creating batches and sending data to the provided Firehose Stream.
func (fb *FirehoseBatcher) Start() error {
	go fb.startBatching()
	return fb.sendBatches(fb.streamName)
}
