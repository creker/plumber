package activemq

import (
	"github.com/Azure/go-amqp"
	"github.com/batchcorp/plumber/cli"
	"github.com/jhump/protoreflect/desc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ActiveMQ holds all attributes required for performing a read/write operations
// in ActiveMQ. This struct should be instantiated via the activemq.Read(..) or
// activemq.Write(..) functions.
type ActiveMQ struct {
	Options  *cli.Options
	Receiver *amqp.Receiver
	Sender   *amqp.Sender
	MsgDesc  *desc.MessageDescriptor
	log      *logrus.Entry
}

func newReader(opts *cli.Options) (*amqp.Receiver, error) {
	ac, err := amqp.Dial(opts.ActiveMQ.Address, amqp.ConnSASLAnonymous())
	if err != nil {
		return nil, err
	}

	session, err := ac.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate session")
	}

	durability := amqp.LinkSourceDurability(amqp.DurabilityNone)
	if opts.ActiveMQ.QueueDurable {
		durability = amqp.LinkSourceDurability(amqp.DurabilityUnsettledState)
	}

	exclusive := ""
	if opts.ActiveMQ.ReadQueueExclusive {
		exclusive = "?consumer.exclusive=true"
	}

	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress(opts.ActiveMQ.Queue+exclusive),
		durability,
	)
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate receiver")
	}

	return receiver, nil
}

func newWriter(opts *cli.Options) (*amqp.Sender, error) {
	ac, err := amqp.Dial(opts.ActiveMQ.Address, amqp.ConnSASLAnonymous())
	if err != nil {
		return nil, err
	}

	session, err := ac.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate session")
	}

	durability := amqp.LinkTargetDurability(amqp.DurabilityNone)
	if opts.ActiveMQ.QueueDurable {
		durability = amqp.LinkTargetDurability(amqp.DurabilityUnsettledState)
	}

	sender, err := session.NewSender(amqp.LinkTargetAddress(opts.ActiveMQ.Queue), durability)
	if err != nil {
		return nil, errors.Wrap(err, "unable to instantiate sender")
	}

	return sender, nil
}
