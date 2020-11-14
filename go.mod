module github.com/jabolina/go-mcast

go 1.14

require (
	github.com/jabolina/relt v0.0.9
	github.com/prometheus/common v0.15.0
	go.uber.org/goleak v1.1.10
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
)

replace (
	github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.5
	github.com/jabolina/relt => /home/jab/master/relt
	google.golang.org/grpc v1.35.0 => google.golang.org/grpc v1.26.0
)
