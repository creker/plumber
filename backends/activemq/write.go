package activemq

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/Azure/go-amqp"
	"github.com/batchcorp/plumber/cli"
	"github.com/batchcorp/plumber/pb"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Write is the entry point function for performing write operations in ActiveMQ.
//
// This is where we verify that the passed args and flags combo makes sense,
// attempt to establish a connection, parse protobuf before finally attempting
// to perform the write.
func Write(opts *cli.Options) error {
	if err := validateWriteOptions(opts); err != nil {
		return errors.Wrap(err, "unable to validate read options")
	}

	var mdErr error
	var md *desc.MessageDescriptor

	if opts.ActiveMQ.WriteOutputType == "protobuf" {
		md, mdErr = pb.FindMessageDescriptor(opts.ActiveMQ.WriteProtobufDir, opts.ActiveMQ.WriteProtobufRootMessage)
		if mdErr != nil {
			return errors.Wrap(mdErr, "unable to find root message descriptor")
		}
	}

	sender, err := newWriter(opts)
	if err != nil {
		return errors.Wrap(err, "unable to complete initial connect")
	}

	r := &ActiveMQ{
		Options: opts,
		Sender:  sender,
		MsgDesc: md,
		log:     logrus.WithField("pkg", "activemq/write.go"),
	}

	msg, err := generateWriteValue(md, opts)
	if err != nil {
		return errors.Wrap(err, "unable to generate write value")
	}

	return r.Write(msg)
}

// Write is a wrapper for amqp Publish method. We wrap it so that we can mock
// it in tests, add logging etc.
func (r *ActiveMQ) Write(value []byte) error {
	return r.Sender.Send(context.Background(), amqp.NewMessage(value))
}

func validateWriteOptions(opts *cli.Options) error {
	if opts.ActiveMQ.Address == "" {
		return errors.New("--address cannot be empty")
	}
	if opts.ActiveMQ.Queue == "" {
		return errors.New("--queue cannot be empty")
	}

	// If output-type is protobuf, ensure that protobuf flags are set
	// If type is protobuf, ensure both --protobuf-dir and --protobuf-root-message
	// are set as well
	if opts.ActiveMQ.WriteOutputType == "protobuf" {
		if opts.ActiveMQ.WriteProtobufDir == "" {
			return errors.New("'--protobuf-dir' must be set when type " +
				"is set to 'protobuf'")
		}

		if opts.ActiveMQ.WriteProtobufRootMessage == "" {
			return errors.New("'--protobuf-root-message' must be when " +
				"type is set to 'protobuf'")
		}

		// Does given dir exist?
		if _, err := os.Stat(opts.ActiveMQ.WriteProtobufDir); os.IsNotExist(err) {
			return fmt.Errorf("--protobuf-dir '%s' does not exist", opts.ActiveMQ.WriteProtobufDir)
		}
	}

	// InputData and file cannot be set at the same time
	if opts.ActiveMQ.WriteInputData != "" && opts.ActiveMQ.WriteInputFile != "" {
		return fmt.Errorf("--input-data and --input-file cannot both be set (choose one!)")
	}

	if opts.ActiveMQ.WriteInputFile != "" {
		if _, err := os.Stat(opts.ActiveMQ.WriteInputFile); os.IsNotExist(err) {
			return fmt.Errorf("--input-file '%s' does not exist", opts.ActiveMQ.WriteInputFile)
		}
	}

	return nil
}

func generateWriteValue(md *desc.MessageDescriptor, opts *cli.Options) ([]byte, error) {
	// Do we read value or file?
	var data []byte

	if opts.ActiveMQ.WriteInputData != "" {
		data = []byte(opts.ActiveMQ.WriteInputData)
	}

	if opts.ActiveMQ.WriteInputFile != "" {
		var readErr error

		data, readErr = ioutil.ReadFile(opts.ActiveMQ.WriteInputFile)
		if readErr != nil {
			return nil, fmt.Errorf("unable to read file '%s': %s", opts.ActiveMQ.WriteInputFile, readErr)
		}
	}

	// Ensure we do not try to operate on a nil md
	if opts.ActiveMQ.WriteOutputType == "protobuf" && md == nil {
		return nil, errors.New("message descriptor cannot be nil when --output-type is protobuf")
	}

	// Input: Plain Output: Plain
	if opts.ActiveMQ.WriteInputType == "plain" && opts.ActiveMQ.WriteOutputType == "plain" {
		return data, nil
	}

	// Input: JSONPB Output: Protobuf
	if opts.ActiveMQ.WriteInputType == "jsonpb" && opts.ActiveMQ.WriteOutputType == "protobuf" {
		var convertErr error

		data, convertErr = convertJSONPBToProtobuf(data, dynamic.NewMessage(md))
		if convertErr != nil {
			return nil, errors.Wrap(convertErr, "unable to convert JSONPB to protobuf")
		}

		return data, nil
	}

	// TODO: Input: Base64 Output: Plain
	// TODO: Input: Base64 Output: Protobuf
	// TODO: And a few more combinations ...

	return nil, errors.New("unsupported input/output combination")
}

// Convert jsonpb -> protobuf -> bytes
func convertJSONPBToProtobuf(data []byte, m *dynamic.Message) ([]byte, error) {
	buf := bytes.NewBuffer(data)

	if err := jsonpb.Unmarshal(buf, m); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal data into dynamic message")
	}

	// Now let's encode that into a proper protobuf message
	pbBytes, err := proto.Marshal(m)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal dynamic protobuf message to bytes")
	}

	return pbBytes, nil
}
