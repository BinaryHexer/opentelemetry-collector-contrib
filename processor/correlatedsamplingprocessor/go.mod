module github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlatedsamplingprocessor

go 1.14

require (
	github.com/CAFxX/fastrand v0.1.0
	github.com/google/uuid v1.2.0
	github.com/pelletier/go-toml v1.8.0 // indirect
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.25.0
	go.uber.org/zap v1.16.0
	google.golang.org/api v0.45.0
	gopkg.in/ini.v1 v1.57.0 // indirect
)

replace github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor => ../tailsamplingprocessor

replace github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal => ../../pkg/batchpersignal
