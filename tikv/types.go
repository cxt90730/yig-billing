package tikv

import (
	"encoding/xml"
	"time"
)

type CorsRule struct {
	Id             string   `xml:"ID"`
	AllowedMethods []string `xml:"AllowedMethod"`
	AllowedOrigins []string `xml:"AllowedOrigin"`
	AllowedHeaders []string `xml:"AllowedHeader"`
	MaxAgeSeconds  int
	ExposedHeaders []string `xml:"ExposeHeader"`
}

type Cors struct {
	XMLName   xml.Name   `xml:"CORSConfiguration" json:"-"`
	CorsRules []CorsRule `xml:"CORSRule"`
}

type Acl struct {
	CannedAcl string
}

type LifecycleRule struct {
	ID         string `xml:"ID"`
	Prefix     string `xml:"Prefix"`
	Status     string `xml:"Status"`
	Expiration string `xml:"Expiration>Days"`
}

type Lifecycle struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rule    []LifecycleRule `xml:"Rule"`
}

type WebsiteConfiguration struct {
	XMLName               xml.Name               `xml:"WebsiteConfiguration"`
	Xmlns                 string                 `xml:"xmlns,attr,omitempty"`
	RedirectAllRequestsTo *RedirectAllRequestsTo `xml:"RedirectAllRequestsTo,omitempty"`
	IndexDocument         *IndexDocument         `xml:"IndexDocument,omitempty"`
	ErrorDocument         *ErrorDocument         `xml:"ErrorDocument,omitempty"`
	RoutingRules          []RoutingRule          `xml:"RoutingRules>RoutingRule,omitempty"`
}

type RedirectAllRequestsTo struct {
	XMLName  xml.Name `xml:"RedirectAllRequestsTo"`
	HostName string   `xml:"HostName"`
	Protocol string   `xml:"Protocol,omitempty"`
}

type IndexDocument struct {
	XMLName xml.Name `xml:"IndexDocument"`
	Suffix  string   `xml:"Suffix"`
}

type ErrorDocument struct {
	XMLName xml.Name `xml:"ErrorDocument"`
	Key     string   `xml:"Key"`
}

type RoutingRule struct {
	XMLName   xml.Name   `xml:"RoutingRule"`
	Condition *Condition `xml:"Condition,omitempty"`
	Redirect  *Redirect  `xml:"Redirect"`
}

type Condition struct {
	XMLName                     xml.Name `xml:"Condition"`
	KeyPrefixEquals             string   `xml:"KeyPrefixEquals,omitempty"`
	HttpErrorCodeReturnedEquals string   `xml:"HttpErrorCodeReturnedEquals,omitempty"`
}

type Redirect struct {
	XMLName              xml.Name `xml:"Redirect"`
	Protocol             string   `xml:"Protocol,omitempty"`
	HostName             string   `xml:"HostName,omitempty"`
	ReplaceKeyPrefixWith string   `xml:"ReplaceKeyPrefixWith,omitempty"`
	ReplaceKeyWith       string   `xml:"ReplaceKeyWith,omitempty"`
	HttpRedirectCode     string   `xml:"HttpRedirectCode,omitempty"`
}

type Bucket struct {
	Name string
	// Date and time when the bucket was created,
	// should be serialized into format "2006-01-02T15:04:05.000Z"
	CreateTime time.Time
	OwnerId    string
	CORS       Cors
	ACL        Acl
	Lifecycle  Lifecycle
	Policy     []byte // need to MarshalJSON
	Website    WebsiteConfiguration
	Versioning string // actually enum: Disabled/Enabled/Suspended
	Usage      int64
}

type Part struct {
	PartNumber int
	Size       int64
	ObjectId   string

	// offset of this part in whole object, calculated when moving parts from
	// `multiparts` table to `objects` table
	Offset               int64
	Etag                 string
	LastModified         string // time string of format "2006-01-02T15:04:05.000Z"
	InitializationVector []byte
}

type SimpleIndex struct {
	Index []int64
}

type ObjectType int

const (
	ObjectTypeNormal     ObjectType = 0
	ObjectTypeAppendable ObjectType = 1
	ObjectTypeMultipart  ObjectType = 2
)

type StorageClass uint8

// Reference：https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/storage-class-intro.html
const (
	// ObjectStorageClassStandard is a ObjectStorageClass enum value
	ObjectStorageClassStandard StorageClass = iota

	// ObjectStorageClassReducedRedundancy is a ObjectStorageClass enum value
	ObjectStorageClassReducedRedundancy

	// ObjectStorageClassGlacier is a ObjectStorageClass enum value
	ObjectStorageClassGlacier

	// ObjectStorageClassStandardIa is a ObjectStorageClass enum value
	ObjectStorageClassStandardIa

	// ObjectStorageClassOnezoneIa is a ObjectStorageClass enum value
	ObjectStorageClassOnezoneIa

	// ObjectStorageClassIntelligentTiering is a ObjectStorageClass enum value
	ObjectStorageClassIntelligentTiering

	// ObjectStorageClassIntelligentTiering is a ObjectStorageClass enum value
	ObjectStorageClassDeepArchive
)

type BackendType uint8

const (
	BackendCeph BackendType = iota
)

const NullVersion = "null"

type Object struct {
	Rowkey           []byte // Rowkey cache
	Name             string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             int64     // file size
	ObjectId         string    // object name in Ceph
	LastModifiedTime time.Time // in format "2006-01-02T15:04:05.000Z"
	Etag             string
	ContentType      string
	CustomAttributes map[string]string
	Parts            map[int]*Part
	PartsIndex       *SimpleIndex
	ACL              Acl
	NullVersion      bool   // if this entry has `null` version
	DeleteMarker     bool   // if this entry is a delete marker
	VersionId        string // version cache
	// type of Server Side Encryption, could be "SSE-KMS", "SSE-S3", "SSE-C"(custom), or ""(none),
	// KMS is not implemented yet
	SseType string
	// encryption key for SSE-S3, the key itself is encrypted with SSE_S3_MASTER_KEY,
	// in AES256-GCM
	EncryptionKey        []byte
	InitializationVector []byte
	// ObjectType include `Normal`, `Appendable`, 'Multipart'
	Type         ObjectType
	StorageClass StorageClass
	CreateTime   uint64 // Timestamp(nanosecond)

	Backend BackendType
	PartNum int
}

type SseRequest struct {
	// type of Server Side Encryption, could be "SSE-KMS", "SSE-S3", "SSE-C"(custom), or ""(none),
	// KMS is not implemented yet
	Type string

	// AWS-managed specific(KMS and S3)
	SseAwsKmsKeyId string
	SseContext     string

	// customer-provided specific(SSE-C)
	SseCustomerAlgorithm string
	SseCustomerKey       []byte

	// keys for copy
	CopySourceSseCustomerAlgorithm string
	CopySourceSseCustomerKey       []byte
}

type MultipartMetadata struct {
	InitiatorId   string
	OwnerId       string
	ContentType   string
	Location      string
	Pool          string
	Acl           Acl
	SseRequest    SseRequest
	EncryptionKey []byte
	CipherKey     []byte
	Attrs         map[string]string
	StorageClass  StorageClass
}

type Multipart struct {
	BucketName  string
	ObjectName  string
	InitialTime uint64
	UploadId    string // upload id cache
	Metadata    MultipartMetadata
	Parts       map[int]*Part
}
