package activemq

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/batchcorp/plumber/printer"
	"github.com/batchcorp/plumber/util"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Read is the entry point function for performing read operations in ActiveMQ.
//
// This is where we verify that the provided arguments and flag combination
// makes sense/are valid; this is also where we will perform our initial conn.
func Read(opts *cli.Options) error {
	if err := validateReadOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ActiveMQ.ReadOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ActiveMQ.ReadProtobufDir, opts.ActiveMQ.ReadProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	receiver, err := newReader(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &ActiveMQ{
		Options:  opts,
		Receiver: receiver,
		MsgDesc:  md,
		log:      logrus.WithField("pkg", "activemq/read.go"),
	}

	return r.Read()
}

// Read will attempt to consume one or more messages from ActiveMQ
//
// NOTE: This method will not tolerate network hiccups. If you plan on running
// this long-term - we should add reconnect support.
func (r *ActiveMQ) Read() error {
	r.log.Info("Listening for message(s) ...")

	lineNumber := 1

	for {
		msg, err := r.Receiver.Receive(context.Background())
		if err != nil {
			return errors.Wrap(err, "unable to receive message")
		}
		if err := msg.Accept(context.Background()); err != nil {
			return errors.Wrap(err, "unable to accept message")
		}

		body := msg.GetData()
		if r.Options.ActiveMQ.ReadOutputType == "protobuf" {
			decoded, err := pb.DecodeProtobufToJSON(dynamic.NewMessage(r.MsgDesc), body)
			if err != nil {
				if !r.Options.ActiveMQ.ReadFollow {
					return fmt.Errorf("unable to decode protobuf message: %s", err)
				}

				printer.Error(fmt.Sprintf("unable to decode protobuf message: %s", err))
				continue
			}

			body = decoded
		}

		var data []byte
		var convertErr error

		switch r.Options.ActiveMQ.ReadConvert {
		case "base64":
			_, convertErr = base64.StdEncoding.Decode(data, body)
		case "gzip":
			data, convertErr = util.Gunzip(body)
		default:
			data = body
		}

		if convertErr != nil {
			if !r.Options.ActiveMQ.ReadFollow {
				return errors.Wrap(convertErr, "unable to complete conversion")
			}

			printer.Error(fmt.Sprintf("unable to complete conversion for message: %s", convertErr))
			continue
		}

		str := string(data)

		if r.Options.ActiveMQ.ReadLineNumbers {
			str = fmt.Sprintf("%d: ", lineNumber) + str
			lineNumber++
		}

		printer.Print(str)

		if !r.Options.ActiveMQ.ReadFollow {
			break
		}
	}

	r.log.Debug("Reader exiting")

	return nil
}

func validateReadOptions(opts *cli.Options) error {
	if opts.ActiveMQ.Address == "" {
		return errors.New("--address cannot be empty")
	}
	if opts.ActiveMQ.Queue == "" {
		return errors.New("--queue cannot be empty")
	}

	if opts.ActiveMQ.ReadOutputType == "protobuf" {
		if opts.ActiveMQ.ReadProtobufDir == "" {
			return errors.New("'--protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.ActiveMQ.ReadProtobufRootMessage == "" {
			return errors.New("'--protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.ActiveMQ.ReadProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("--protobuf-dir '%s' does not exist", opts.ActiveMQ.ReadProtobufDir)
		}
	}

	return nil
}
