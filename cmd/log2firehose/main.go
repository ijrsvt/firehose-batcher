package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosebatcher "github.com/ijrsvt/firehose-batcher"
	flags "github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thomaso-mirodin/tailer"
)

type Flags struct {
	Path string `short:"f" long:"filename" default:"-" description:"Path to file to tail, defaults to stdin"`
	Args struct {
		FirehoseStreamName string `required:"true" positional-arg-name:"firehose_stream_name"`
	} `positional-args:"true"`

	cfg struct {
		source   io.Reader
		firehose *firehose.Client
	}
}

func ParseFlags() (*Flags, error) {
	f := new(Flags)
	_, err := flags.Parse(f)
	if err != nil {
		return nil, err
	}

	switch f.Path {
	case "-":
		f.cfg.source = os.Stdin
	default:
		var err error
		f.cfg.source, err = tailer.NewFile(
			f.Path,
			tailer.SetBufferSize(4*1024*1024),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create tailer")
		}
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create AWS config")
	}
	f.cfg.firehose = firehose.NewFromConfig(cfg)

	return f, nil
}

func bufferedLineSplitter(in io.Reader) chan []byte {
	scanner := bufio.NewScanner(in)
	out := make(chan []byte)

	go func() {
		defer close(out)

		for scanner.Scan() {
			rawBytes := scanner.Bytes()

			buf := make([]byte, len(rawBytes))
			copy(buf, rawBytes)

			out <- buf
		}

		switch err := scanner.Err(); err {
		case nil, io.EOF:
			return
		default:
			log.Println(errors.Wrap(err, "scanner returned error"))
		}
	}()

	return out
}

func main() {
	f, err := ParseFlags()
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to parse flags"))
	}

	batcher, err := firehosebatcher.New(
		f.cfg.firehose,
		f.Args.FirehoseStreamName,
		time.Second*60,
	)
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to create firehose batcher"))
	}

	firehosebatcher.RegisterMetrics(prometheus.DefaultRegisterer)
	go func() {
		http.Handle("/metrics", prometheus.Handler())
		http.ListenAndServe("127.0.0.1:8080", nil)
	}()

	go func() {
		err := batcher.Start()
		if err != nil {
			log.Fatal(errors.Wrap(err, "batch sending exited early"))
		}
	}()

	lines := bufferedLineSplitter(f.cfg.source)
	if err := batcher.AddRawFromChan(lines); err != nil {
		log.Println(errors.Wrap(err, "adding lines failed"))
	}
}
