package messagebus

import "github.com/confluentinc/confluent-kafka-go/kafka"

const (
	TimeLocal          = "time_local"
	RequestUrl         = "request_uri"
	RequestId          = "request_id"
	OperationName      = "operation_name"
	HostName           = "host_name"
	BucketName         = "bucket_name"
	ObjectName         = "object_name"
	ObjectSize         = "object_size"
	RequesterId        = "requester_id"
	ProjectId          = "project_id"
	RemoteAddress      = "remote_addr"
	HttpXRealIp        = "http_x_real_ip"
	RequestLength      = "request_length"
	ServerCost         = "server_cost"
	RequestTime        = "request_time"
	HttpStatus         = "http_status"
	ErrorCode          = "error_code"
	BodyBytesSent      = "body_bytes_sent"
	HttpReferer        = "http_referer"
	HttpUserAgent      = "http_user_agent"
	IsPrivateSubnet    = "is_private_subnet"
	StorageClass       = "storage_class"
	TargetStorageClass = "target_storage_class"
	BucketLogging      = "bucket_logging"
	CdnRequest         = "cdn_request"
)

type Kafka struct {
	consumer *kafka.Consumer
}

type ConsumerMessage struct {
	Uuid     string
	Messages map[string]string
}

var KafkaConsumer *Kafka
var ConsumerMessagePipe chan ConsumerMessage
