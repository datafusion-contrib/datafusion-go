package datafusion

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

func init() {
	sql.Register("datafusion", Driver{})
}

// Driver implements database/sql/driver.Driver for DataFusion.
type Driver struct{}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := connector.Connect(context.Background())
	if err != nil {
		if closer, ok := connector.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	return conn, nil
}

func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn, nil)
}

// ConnectorOption configures a DataFusion connector.
type ConnectorOption func(*connectorOptions)

type connectorOptions struct {
	sharedSession bool
}

func defaultConnectorOptions() connectorOptions {
	return connectorOptions{sharedSession: true}
}

// WithSharedSession controls whether connections from one Connector share a DataFusion SessionContext.
func WithSharedSession(shared bool) ConnectorOption {
	return func(opts *connectorOptions) {
		opts.sharedSession = shared
	}
}

// Connector owns the native DataFusion database handle used to open pooled connections.
type Connector struct {
	db            *native.Database
	initFn        func(context.Context, driver.ExecerContext) error
	sharedSession bool

	mu                sync.Mutex
	sharedInitMu      sync.Mutex
	ddlMu             sync.Mutex
	closed            bool
	sharedInitialized bool
}

// NewConnector creates a DataFusion connector for database/sql.
func NewConnector(dsn string, initFn func(driver.ExecerContext) error) (*Connector, error) {
	if initFn == nil {
		return NewConnectorWithInitContext(dsn, nil)
	}
	return NewConnectorWithInitContext(dsn, func(_ context.Context, exec driver.ExecerContext) error {
		return initFn(exec)
	})
}

// NewConnectorWithInitContext creates a DataFusion connector with an optional
// initialization callback and connector options.
func NewConnectorWithInitContext(dsn string, initFn func(context.Context, driver.ExecerContext) error, options ...ConnectorOption) (*Connector, error) {
	opts := defaultConnectorOptions()
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}

	normalized, err := normalizeDSN(dsn, &opts)
	if err != nil {
		return nil, driverError(ErrorConnect, "could not parse datafusion DSN", err)
	}

	db, err := native.OpenDatabase(normalized)
	if err != nil {
		return nil, driverError(ErrorConnect, "could not open DataFusion database", err)
	}

	return &Connector{
		db:            db,
		initFn:        initFn,
		sharedSession: opts.sharedSession,
	}, nil
}

// Driver returns the database/sql driver used by the connector.
func (c *Connector) Driver() driver.Driver {
	return Driver{}
}

// Connect opens a new DataFusion connection.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	nc, err := c.connectNative(ctx)
	if err != nil {
		return nil, err
	}

	conn := newConn(nc, c)
	if c.initFn != nil {
		if c.sharedSession {
			if err := c.initializeShared(ctx, conn); err != nil {
				_ = conn.Close()
				return nil, err
			}
		} else {
			if err := c.initFn(ctx, conn); err != nil {
				_ = conn.Close()
				return nil, err
			}
		}
	}

	return conn, nil
}

func (c *Connector) connectNative(ctx context.Context) (*native.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, driverError(ErrorClosed, "datafusion connector is closed", nil)
	}

	nc, err := c.db.Connect(c.sharedSession)
	if err != nil {
		return nil, driverError(ErrorConnect, "could not connect to DataFusion database", err)
	}

	return nc, nil
}

func (c *Connector) initializeShared(ctx context.Context, conn driver.ExecerContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	c.sharedInitMu.Lock()
	defer c.sharedInitMu.Unlock()

	if c.sharedInitialized {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return driverError(ErrorClosed, "datafusion connector is closed", nil)
	}
	c.mu.Unlock()

	if err := c.initFn(ctx, conn); err != nil {
		return err
	}
	c.sharedInitialized = true
	return nil
}

func (c *Connector) lockSerializedStatement(ctx context.Context, serializes bool) (func(), error) {
	if c == nil || !serializes {
		return nil, nil
	}

	c.ddlMu.Lock()
	if err := ctx.Err(); err != nil {
		c.ddlMu.Unlock()
		return nil, err
	}
	return c.ddlMu.Unlock, nil
}

// Close releases the connector's DataFusion database resources.
func (c *Connector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	c.db.Close()
	return nil
}

func normalizeDSN(dsn string, opts *connectorOptions) (string, error) {
	const supportedDSNs = "supported DSNs are empty string, ?<options>, datafusion://, and datafusion://?<options>"

	if dsn == "" {
		return "", nil
	}
	if strings.HasPrefix(dsn, "?") {
		return normalizeSessionDSN(dsn[1:], opts)
	}
	if strings.HasPrefix(dsn, "datafusion://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		if parsed.Scheme != "datafusion" {
			return "", fmt.Errorf("unsupported DataFusion DSN %q; %s", dsn, supportedDSNs)
		}
		if parsed.Host != "" {
			return "", fmt.Errorf("unsupported DataFusion DSN %q; datafusion:// DSNs do not support hosts", dsn)
		}
		if parsed.Path != "" && parsed.Path != "/" {
			return "", fmt.Errorf("unsupported DataFusion DSN %q; datafusion:// DSNs only support an empty or / path", dsn)
		}
		return normalizeSessionDSN(parsed.RawQuery, opts)
	}
	return "", fmt.Errorf("unsupported DataFusion DSN %q; %s", dsn, supportedDSNs)
}

func normalizeSessionDSN(rawQuery string, opts *connectorOptions) (string, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", err
	}

	for key, value := range values {
		switch key {
		case "datafusion.go.shared_session":
			shared, err := parseBoolOption(key, value)
			if err != nil {
				return "", err
			}
			opts.sharedSession = shared
			delete(values, key)
		}
	}

	encoded := values.Encode()
	if encoded == "" {
		return "", nil
	}
	return "?" + encoded, nil
}

func parseBoolOption(key string, values []string) (bool, error) {
	if len(values) == 0 || values[0] == "" {
		return false, fmt.Errorf("%s requires a boolean value", key)
	}
	if len(values) > 1 {
		return false, fmt.Errorf("%s must be specified at most once", key)
	}
	switch strings.ToLower(values[0]) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean value, got %q", key, values[0])
	}
}

var _ driver.Driver = Driver{}
var _ driver.DriverContext = Driver{}
var _ driver.Connector = (*Connector)(nil)
var _ interface{ Close() error } = (*Connector)(nil)
