package firehosebatcher

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/stretchr/testify/assert"
)

func TestPackRecordBatch(t *testing.T) {
	assert := assert.New(t)

	b := NewBatch(types.Record{
		Data: []byte{},
	})

	r := types.Record{
		Data: bytes.Repeat([]byte("A"), PER_ITEM_SIZE_LIMIT/2),
	}
	assert.NoError(b.Add(r, true))
	// Should pack to the first record
	assert.Equal(1, len(b.contents))

	assert.NoError(b.Add(r, true))
	// Second add should go to a new record
	assert.Equal(2, len(b.contents))

	// Ensure that the first record is smaller than the SIZE_LIMIT & that there is no extraneous newlines
	assert.True(len(b.contents[0].Data) < PER_ITEM_SIZE_LIMIT)
	assert.True(bytes.Count(b.contents[0].Data, []byte("\n")) == 0)

	// Ensure that there are new line delimiters
	assert.NoError(b.Add(types.Record{
		Data: []byte("something"),
	}, true))
	assert.True(bytes.Count(b.contents[1].Data, []byte("\n")) == 1)

}
