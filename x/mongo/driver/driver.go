package driver // import "go.mongodb.org/mongo-driver/x/mongo/driver"

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// Deployment is implemented by types that can select a server from a deployment.
type Deployment interface {
	SelectServer(context.Context, description.ServerSelector) (Server, error)
	Kind() description.TopologyKind
}

// Connector represents a type that can connect to a server.
type Connector interface {
	Connect() error
}

// Disconnector represents a type that can disconnect from a server.
type Disconnector interface {
	Disconnect(context.Context) error
}

// Subscription represents a subscription to topology updates. A subscriber can receive updates through the
// Updates field.
type Subscription struct {
	Updates <-chan description.Topology
	ID      uint64
}

// Subscriber represents a type to which another type can subscribe. A subscription contains a channel that
// is updated with topology descriptions.
type Subscriber interface {
	Subscribe() (*Subscription, error)
	Unsubscribe(*Subscription) error
}

// Server represents a MongoDB server. Implementations should pool connections and handle the
// retrieving and returning of connections.
type Server interface {
	Connection(context.Context) (Connection, error)
}

// Connection represents a connection to a MongoDB server.
type Connection interface {
	WriteWireMessage(context.Context, []byte) error
	ReadWireMessage(ctx context.Context, dst []byte) ([]byte, error)
	Description() description.Server
	Close() error
	ID() string
	Address() address.Address
	Stale() bool
}

// LocalAddresser is a type that is able to supply its local address
type LocalAddresser interface {
	LocalAddress() address.Address
}

// Expirable represents an expirable object.
type Expirable interface {
	Expire() error
	Alive() bool
}

// StreamerConnection represents a Connection that supports streaming wire protocol messages using the moreToCome and
// exhaustAllowed flags.
//
// The SetStreaming and CurrentlyStreaming functions correspond to the moreToCome flag on server responses. If a
// response has moreToCome set, SetStreaming(true) will be called and CurrentlyStreaming() should return true.
//
// CanStream corresponds to the exhaustAllowed flag. The operations layer will set exhaustAllowed on outgoing wire
// messages to inform the server that the driver supports streaming.
type StreamerConnection interface {
	Connection
	SetStreaming(bool)
	CurrentlyStreaming() bool
	SupportsStreaming() bool
}

// Compressor is an interface used to compress wire messages. If a Connection supports compression
// it should implement this interface as well. The CompressWireMessage method will be called during
// the execution of an operation if the wire message is allowed to be compressed.
type Compressor interface {
	CompressWireMessage(src, dst []byte) ([]byte, error)
}

// ErrorProcessor implementations can handle processing errors, which may modify their internal state.
// If this type is implemented by a Server, then Operation.Execute will call it's ProcessError
// method after it decodes a wire message.
type ErrorProcessor interface {
	ProcessError(err error, conn Connection)
}

// HandshakeInformation contains information extracted from a MongoDB connection handshake. This is a helper type that
// augments description.Server by also tracking authentication-related fields. We use this type rather than adding
// these fields to description.Server to avoid retaining sensitive information in a user-facing type.
type HandshakeInformation struct {
	Description             description.Server
	SpeculativeAuthenticate bsoncore.Document
	SaslSupportedMechs      []string
}

// Handshaker is the interface implemented by types that can perform a MongoDB
// handshake over a provided driver.Connection. This is used during connection
// initialization. Implementations must be goroutine safe.
type Handshaker interface {
	GetHandshakeInformation(context.Context, address.Address, Connection) (HandshakeInformation, error)
	FinishHandshake(context.Context, Connection) error
}

// SingleServerDeployment is an implementation of Deployment that always returns a single server.
type SingleServerDeployment struct{ Server }

var _ Deployment = SingleServerDeployment{}

// SelectServer implements the Deployment interface. This method does not use the
// description.SelectedServer provided and instead returns the embedded Server.
func (ssd SingleServerDeployment) SelectServer(context.Context, description.ServerSelector) (Server, error) {
	return ssd.Server, nil
}

// Kind implements the Deployment interface. It always returns description.Single.
func (SingleServerDeployment) Kind() description.TopologyKind { return description.Single }

// SingleConnectionDeployment is an implementation of Deployment that always returns the same Connection. This
// implementation should only be used for connection handshakes and server heartbeats as it does not implement
// ErrorProcessor, which is necessary for application operations.
type SingleConnectionDeployment struct{ C Connection }

var _ Deployment = SingleConnectionDeployment{}
var _ Server = SingleConnectionDeployment{}

// SelectServer implements the Deployment interface. This method does not use the
// description.SelectedServer provided and instead returns itself. The Connections returned from the
// Connection method have a no-op Close method.
func (ssd SingleConnectionDeployment) SelectServer(context.Context, description.ServerSelector) (Server, error) {
	return ssd, nil
}

// Kind implements the Deployment interface. It always returns description.Single.
func (ssd SingleConnectionDeployment) Kind() description.TopologyKind { return description.Single }

// Connection implements the Server interface. It always returns the embedded connection.
func (ssd SingleConnectionDeployment) Connection(context.Context) (Connection, error) {
	return ssd.C, nil
}

// TODO(GODRIVER-617): We can likely use 1 type for both the Type and the RetryMode by using
// 2 bits for the mode and 1 bit for the type. Although in the practical sense, we might not want to
// do that since the type of retryability is tied to the operation itself and isn't going change,
// e.g. and insert operation will always be a write, however some operations are both reads and
// writes, for instance aggregate is a read but with a $out parameter it's a write.

// Type specifies whether an operation is a read, write, or unknown.
type Type uint

// THese are the availables types of Type.
const (
	_ Type = iota
	Write
	Read
)
