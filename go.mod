module github.com/jabolina/go-mcast

go 1.14

require (
	github.com/coocood/freecache v1.1.1
	github.com/digital-comrades/proletariat v1.0.8
	github.com/jabolina/relt v1.0.0
	github.com/prometheus/common v0.15.0
	go.uber.org/goleak v1.1.10
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
)

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5
	google.golang.org/grpc v1.35.0 => google.golang.org/grpc v1.26.0
)
