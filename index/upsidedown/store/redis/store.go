package redis

import (
	"fmt"

	"github.com/go-redis/redis/v8"

	"github.com/binhjax/bleve/v2/registry"
	store "github.com/blevesearch/upsidedown_store_api"
)

const (
	// Name of the Key/Value Store within the registry
	Name = "redis"
)

// Store implements the KVStore interface for a Redis.
type Store struct {
	conn *redis.Client
	mo   store.MergeOperator
}

// New creates a new KVStore which persists its data on Redis.
// It requires at least a config value for "url". Further config values are:
//   - db(optional)
//     The database number. Must be parseable to an int
//   - connection_timeout (optional)
//     The timeout after which a connection attempt to Redis is abandoned.
//     Must be parseable to time.Duration
//   - read_timeout (optional)
//     The timeout after which a single read operation is abandoned.
//     Must be parseable to time.Duration
//   - write_timeout (optional)
//     The timeout after which a single write operation is abandoned.
//     Must be parseable to time.Duration
//   - use_tls (optional)
//     Wether to use TLS to connect to Redis or not.
//     Must be parseable to a boolean.
//   - tls_skip_verify (optional)
//     Wether to validate the server's certificate or not.
//     Only evaluated when use_tls is true.
//     Must be parseable to a boolean.
//   - password (optional)
//     String containing the password for Redis.
func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	var err error
	s := Store{}
	s.conn, err = connect(config)
	if err != nil {

	}
	s.mo = mo
	return s, nil
}

func (s Store) Reader() (store.KVReader, error) {
	return Reader{store: &s}, nil
}

func (s Store) Writer() (store.KVWriter, error) {
	return Writer{store: &s}, nil
}

// Close flushes the connection to Redis and closes it.
func (s Store) Close() (err error) {
	// if err = s.conn.Flush(); err != nil {
	// 	return fmt.Errorf("Error flushing datastore: %s", err)
	// }

	if err = s.conn.Close(); err != nil {
		return fmt.Errorf("Error closing connection to datastore: %s", err)
	}

	return nil
}

type stats struct {
	value string
}

func connect(config map[string]interface{}) (*redis.Client, error) {
	opt := &redis.Options{}
	// // The Redis URL we connect to
	// // We need to make this function-global, as we want to fail early
	// // in case it is not set, but after all other options have been checked and set.
	// var url string

	// // We need to make this function-global, as in case TLS is not used
	// // we need to warn the user that sending a password is highly insecure.
	// var useTLS bool

	// dialOpts := make([]redis.DialOption, 0)

	// if url, ok := config["url"].(string); !ok {
	// 	return nil, fmt.Errorf("must specify url")
	// }

	// if db, ok := config["db"].(string); !ok {
	// 	dbNum, err := strconv.Atoi(db)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Value '%s' for db can not be parsed: %s", db, err)
	// 	}
	// 	dialOpts = append(dialOpts, redis.DialDatabase(dbNum))
	// }

	// // Add connection timeout to dial options, if set.
	// if ct, ok := config["connection_timeout"].(string); ok {
	// 	d, err := time.ParseDuration(ct)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Duration '%s' for connection_timeout can not be parsed: %s", ct, err)
	// 	}
	// 	dialOpts = append(dialOpts, redis.DialConnectTimeout(d))
	// }

	// // Add read timeout to dial options, if set.
	// // See https://godoc.org/github.com/gomodule/redigo/redis#DialReadTimeout for details
	// if rt, ok := config["read_timeout"].(string); ok {
	// 	d, err := time.ParseDuration(rt)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Duration '%s' for read_timeout can not be parsed: %s", rt, err)
	// 	}
	// 	dialOpts = append(dialOpts, redis.DialReadTimeout(d))
	// }

	// // Add a write timeout to dial options, if set.
	// // See https://godoc.org/github.com/gomodule/redigo/redis#DialWriteTimeout for details
	// if wt, ok := config["write_timeout"].(string); ok {
	// 	d, err := time.ParseDuration(wt)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Duration '%s' for write_timeout can not be parsed: %s", wt, err)
	// 	}
	// 	dialOpts = append(dialOpts, redis.DialWriteTimeout(d))
	// }

	// // Use TLS, if required, and set the according skip_verify, if set.
	// if tls, ok := config["use_tls"].(string); ok {
	// 	useTLS, err := strconv.ParseBool(tls)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Value '%s' for use_tls can not be parsed: %s", tls, err)
	// 	}

	// 	// Set the according dial options only if TLS is requested
	// 	if useTLS {
	// 		dialOpts = append(dialOpts, redis.DialUseTLS(useTLS))

	// 		if noverify, ok := config["tls_skip_verify"].(string); ok {
	// 			skip, err := strconv.ParseBool(noverify)
	// 			if err != nil {
	// 				return nil, fmt.Errorf("Value '%s' for tls_skip_verify can not be parsed: %s", noverify, err)
	// 			}

	// 			// No conditional append here, as explicit false should to be obeyed.
	// 			dialOpts = append(dialOpts, redis.DialTLSSkipVerify(skip))
	// 		}
	// 	}
	// }

	// // Add password to dial options, if set
	// if pass, ok := config["password"].(string); ok {

	// 	if !useTLS {
	// 		// Probably, an error _should_ be returned instead of
	// 		// just logging a warning message. But then, this would make
	// 		// testing authentication against a simple local instance impossible.
	// 		log.Println("Your configuration has a password, but does not use TLS!")
	// 		log.Println("Your password will be sent over the network in PLAIN, human readable form!")
	// 	}
	// 	dialOpts = append(dialOpts, redis.DialPassword(pass))
	// }

	// return redis.DialURL(url, dialOpts...)
	return redis.NewClient(opt), nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
