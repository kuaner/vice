// Package nats provides a Vice implementation for NATS.
package nats

import (
	"github.com/matryer/vice"
	"github.com/nats-io/go-nats"
	"golang.org/x/sync/syncmap"
)

// make sure Transport satisfies vice.Transport interface.
var _ vice.Transport = (*Transport)(nil)

// Transport is a vice.Transport for NATS queue.
type Transport struct {
	receiveChans syncmap.Map

	sendChans syncmap.Map

	errChan chan error
	// stopchan is closed when everything has stopped.
	stopchan chan struct{}
	//stopPubChan chan struct{}

	subscriptions []*nats.Subscription
	nc            *nats.Conn
	natsAddr      string
}

// New returns a new Transport
func New(natsURL string) (vice.Transport, error) {
	//TODO bind nats connectiong handler
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}
	return &Transport{
		receiveChans: syncmap.Map{},
		sendChans:    syncmap.Map{},

		errChan:  make(chan error, 10),
		stopchan: make(chan struct{}),

		subscriptions: []*nats.Subscription{},
		nc:            nc,
		natsAddr:      natsURL,
	}, nil
}

// Receive gets a channel on which to receive messages
// with the specified name.
func (t *Transport) Receive(name string) <-chan []byte {
	ch := make(chan []byte)
	val, ok := t.receiveChans.Load(name)
	if ok {
		return val.(chan []byte)
	}

	ch, err := t.makeSubscriber(name)
	if err != nil {
		t.errChan <- vice.Err{Name: name, Err: err}
	}

	t.receiveChans.Store(name, ch)
	return ch
}

func (t *Transport) makeSubscriber(name string) (chan []byte, error) {
	ch := make(chan []byte)

	sub, err := t.nc.Subscribe(name, func(m *nats.Msg) {
		data := m.Data
		ch <- data
		return
	})
	if err != nil {
		return nil, err
	}
	t.subscriptions = append(t.subscriptions, sub)
	return ch, nil
}

// Send gets a channel on which messages with the
// specified name may be sent.
func (t *Transport) Send(name string) chan<- []byte {

	ch := make(chan []byte)
	val, ok := t.sendChans.Load(name)
	if ok {
		return val.(chan []byte)
	}

	ch, err := t.makePublisher(name)
	if err != nil {
		t.errChan <- vice.Err{Name: name, Err: err}
		return make(chan []byte)
	}

	t.sendChans.Store(name, ch)
	return ch
}

func (t *Transport) makePublisher(name string) (chan []byte, error) {
	ch := make(chan []byte)

	go func() {
		for {
			select {
			case msg := <-ch:
				if err := t.nc.Publish(name, msg); err != nil {
					t.errChan <- vice.Err{Message: msg, Name: name, Err: err}
					continue
				}
			default:
			}
		}
	}()
	return ch, nil
}

// ErrChan gets the channel on which errors are sent.
func (t *Transport) ErrChan() <-chan error {
	return t.errChan
}

// Stop stops the transport.
// The channel returned from Done() will be closed
// when the transport has stopped.
func (t *Transport) Stop() {
	for _, s := range t.subscriptions {
		s.Unsubscribe()
	}
	t.nc.Close()
	close(t.stopchan)
}

// Done gets a channel which is closed when the
// transport has successfully stopped.
func (t *Transport) Done() chan struct{} {
	return t.stopchan
}
