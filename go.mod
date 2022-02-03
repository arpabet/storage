module go.arpabet.com/storage

go 1.14

replace github.com/dgraph-io/badger/v2 => github.com/arpabet/badger/v2 v2.0.3-mu

require (
	github.com/boltdb/bolt v1.3.1
	github.com/cockroachdb/pebble v0.0.0-20210709205950-ea60b4722cca
	github.com/golang/protobuf v1.4.2
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pingcap/errors v0.11.4
	github.com/pkg/errors v0.9.1
	github.com/dgraph-io/badger/v2 v2.0.3
	go.arpabet.com/beans v1.0.0
	go.arpabet.com/value v0.6.0
	go.etcd.io/bbolt v1.3.6
	go.uber.org/zap v1.15.0
)
