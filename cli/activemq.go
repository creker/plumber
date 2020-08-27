package cli

import "gopkg.in/alecthomas/kingpin.v2"

type ActiveMQOptions struct {
	// Shared
	Address      string
	Queue        string
	QueueDurable bool

	// Read
	ReadQueueExclusive      bool
	ReadLineNumbers         bool
	ReadFollow              bool
	ReadProtobufDir         string
	ReadProtobufRootMessage string
	ReadOutputType          string
	ReadConvert             string

	// Write
	WriteInputData           string
	WriteInputFile           string
	WriteInputType           string
	WriteOutputType          string
	WriteProtobufDir         string
	WriteProtobufRootMessage string
}

func HandleActiveMQFlags(readCmd, writeCmd *kingpin.CmdClause, opts *Options) {
	// ActiveMQ read cmd
	rc := readCmd.Command("activemq", "ActiveMQ message system")

	addSharedActiveMQFlags(rc, opts)
	addReadActiveMQFlags(rc, opts)

	// ActiveMQ write cmd
	wc := writeCmd.Command("activemq", "ActiveMQ message system")

	addSharedActiveMQFlags(wc, opts)
	addWriteActiveMQFlags(wc, opts)
}

func addSharedActiveMQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("address", "Destination host address").Default("amqp://localhost:5672").
		StringVar(&opts.ActiveMQ.Address)
	cmd.Flag("queue", "Name of the queue").
		StringVar(&opts.ActiveMQ.Queue)
	cmd.Flag("queue-durable", "Whether the queue we declare should survive server restarts").
		Default("false").BoolVar(&opts.ActiveMQ.QueueDurable)
}

func addReadActiveMQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("queue-exclusive", "Whether plumber should be the only one using the newly defined queue").
		Default("true").BoolVar(&opts.ActiveMQ.ReadQueueExclusive)
	cmd.Flag("line-numbers", "Display line numbers for each message").
		Default("false").BoolVar(&opts.ActiveMQ.ReadLineNumbers)
	cmd.Flag("follow", "Continuous read (ie. `tail -f`)").Short('f').
		BoolVar(&opts.ActiveMQ.ReadFollow)
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.ActiveMQ.ReadProtobufDir)
	cmd.Flag("protobuf-root-message", "Specifies the root message in a protobuf descriptor "+
		"set (required if protobuf-dir set)").StringVar(&opts.ActiveMQ.ReadProtobufRootMessage)
	cmd.Flag("output-type", "The type of message(s) you will receive on the bus").
		Default("plain").EnumVar(&opts.ActiveMQ.ReadOutputType, "plain", "protobuf")
	cmd.Flag("convert", "Convert received (output) message(s)").
		EnumVar(&opts.ActiveMQ.ReadConvert, "base64", "gzip")
}

func addWriteActiveMQFlags(cmd *kingpin.CmdClause, opts *Options) {
	cmd.Flag("input-data", "Data to write to ActiveMQ").StringVar(&opts.ActiveMQ.WriteInputData)
	cmd.Flag("input-file", "File containing input data (overrides input-data; 1 file is 1 message)").
		ExistingFileVar(&opts.ActiveMQ.WriteInputFile)
	cmd.Flag("input-type", "Treat input as this type").Default("plain").
		EnumVar(&opts.ActiveMQ.WriteInputType, "plain", "base64", "jsonpb")
	cmd.Flag("output-type", "Convert input to this type when writing message").
		Default("plain").EnumVar(&opts.ActiveMQ.WriteOutputType, "plain", "protobuf")
	cmd.Flag("protobuf-dir", "Directory with .proto files").
		ExistingDirVar(&opts.ActiveMQ.WriteProtobufDir)
	cmd.Flag("protobuf-root-message", "Root message in a protobuf descriptor set "+
		"(required if protobuf-dir set)").StringVar(&opts.ActiveMQ.WriteProtobufRootMessage)
}
