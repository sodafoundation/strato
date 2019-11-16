package types

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/meta/util"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/xxtea/xxtea-go/xxtea"
)

type Object struct {
	*pb.Object
}

type ObjectType string

const (
	ObjectTypeNormal = iota
	ObjectTypeAppendable
	ObjectTypeMultipart
)

func (o *Object) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	body, err := helper.MsgPackMarshal(o)
	if err != nil {
		return nil, err
	}
	fields[FIELD_NAME_BODY] = string(body)
	return fields, nil
}

func (o *Object) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s", FIELD_NAME_BODY))
	}

	err := helper.MsgPackUnMarshal([]byte(body), o)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (o *Object) ObjectTypeToString() string {
	switch o.Type {
	case ObjectTypeNormal:
		return "Normal"
	case ObjectTypeAppendable:
		return "Appendable"
	case ObjectTypeMultipart:
		return "Multipart"
	default:
		return "Unknown"
	}
}

func (o *Object) String() (s string) {
	s += "Name: " + o.ObjectKey + "\n"
	s += "Bucket: " + o.BucketName + "\n"
	s += "Location: " + o.Location + "\n"
	//s += "Pool: " + o.Pool + "\n"
	s += "Object ID: " + o.ObjectId + "\n"
	s += "Last Modified Time: " + time.Unix(o.LastModified, 0).Format(CREATE_TIME_LAYOUT) + "\n"
	s += "Version: " + o.VersionId + "\n"
	s += "Type: " + o.ObjectTypeToString() + "\n"
	s += "Tier: " + fmt.Sprintf("%d", o.Tier) + "\n"
	// TODO: multi-part handle

	return s
}

func (o *Object) GetVersionNumber() (uint64, error) {
	decrypted, err := util.Decrypt(o.VersionId)
	if err != nil {
		return 0, err
	}
	version, err := strconv.ParseUint(decrypted, 10, 64)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (o *Object) GetValues() (values map[string]map[string][]byte, err error) {
	var size, tier bytes.Buffer
	err = binary.Write(&size, binary.BigEndian, o.Size)
	if err != nil {
		return
	}
	err = binary.Write(&tier, binary.BigEndian, o.Tier)
	if err != nil {
		return
	}
	err = o.encryptSseKey()
	if err != nil {
		return
	}
	if o.ServerSideEncryption.EncryptionKey == nil {
		o.ServerSideEncryption.EncryptionKey = []byte{}
	}
	if o.ServerSideEncryption.InitilizationVector == nil {
		o.ServerSideEncryption.InitilizationVector = []byte{}
	}
	var attrsData []byte
	if o.CustomAttributes != nil {
		attrsData, err = json.Marshal(o.CustomAttributes)
		if err != nil {
			return
		}
	}
	values = map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY: map[string][]byte{
			"bucket":        []byte(o.BucketName),
			"location":      []byte(o.Location),
			"owner":         []byte(o.UserId),
			"oid":           []byte(o.ObjectId),
			"size":          size.Bytes(),
			"lastModified":  []byte(time.Unix(o.LastModified, 0).Format(CREATE_TIME_LAYOUT)),
			"etag":          []byte(o.Etag),
			"content-type":  []byte(o.ContentType),
			"attributes":    attrsData, // TODO
			"ACL":           []byte(o.Acl.CannedAcl),
			"nullVersion":   []byte(helper.Ternary(o.NullVersion, "true", "false").(string)),
			"deleteMarker":  []byte(helper.Ternary(o.DeleteMarker, "true", "false").(string)),
			"sseType":       []byte(o.ServerSideEncryption.SseType),
			"encryptionKey": o.ServerSideEncryption.EncryptionKey,
			"IV":            o.ServerSideEncryption.InitilizationVector,
			"type":          []byte(o.ObjectTypeToString()),
			"tier":          tier.Bytes(),
		},
	}
	// TODO: multipart handle

	return
}

func (o *Object) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJECT_COLUMN_FAMILY:      map[string][]byte{},
		OBJECT_PART_COLUMN_FAMILY: map[string][]byte{},
	}
}

func (o *Object) encryptSseKey() (err error) {
	// Don't encrypt if `EncryptionKey` is not set
	if len(o.ServerSideEncryption.EncryptionKey) == 0 {
		return
	}

	if len(o.ServerSideEncryption.InitilizationVector) == 0 {
		o.ServerSideEncryption.InitilizationVector = make([]byte, INITIALIZATION_VECTOR_LENGTH)
		_, err = io.ReadFull(rand.Reader, o.ServerSideEncryption.InitilizationVector)
		if err != nil {
			return
		}
	}

	block, err := aes.NewCipher(SSE_S3_MASTER_KEY)
	if err != nil {
		return err
	}

	aesGcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	// InitializationVector is 16 bytes(because of CTR), but use only first 12 bytes in GCM
	// for performance
	o.ServerSideEncryption.EncryptionKey = aesGcm.Seal(nil, o.ServerSideEncryption.InitilizationVector[:12], o.ServerSideEncryption.EncryptionKey, nil)
	return nil
}

func (o *Object) GetVersionId() string {
	if o.NullVersion {
		return "null"
	}
	if o.VersionId != "" {
		return o.VersionId
	}
	timeData := []byte(strconv.FormatUint(uint64(o.LastModified), 10))
	o.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return o.VersionId
}

//Tidb related function

func (o *Object) GetCreateSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModified)
	customAttributes, _ := json.Marshal(o.CustomAttributes)
	acl, _ := json.Marshal(o.Acl)
	var sseType string
	var encryptionKey, initVector []byte
	if o.ServerSideEncryption != nil {
		sseType = o.ServerSideEncryption.SseType
		encryptionKey = o.ServerSideEncryption.EncryptionKey
		initVector = o.ServerSideEncryption.InitilizationVector
	}

	lastModifiedTime := time.Unix(o.LastModified, 0).Format(TIME_LAYOUT_TIDB)
	sql := "insert into objects (bucketname, name, version, location, tenantid, userid, size, objectid, " +
		" lastmodifiedtime, etag, contenttype, customattributes, acl, nullversion, deletemarker, ssetype, " +
		" encryptionkey, initializationvector, type, tier, storageMeta) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	args := []interface{}{o.BucketName, o.ObjectKey, version, o.Location, o.TenantId, o.UserId, o.Size, o.ObjectId,
		lastModifiedTime, o.Etag, o.ContentType, customAttributes, acl, o.NullVersion, o.DeleteMarker, sseType,
		encryptionKey, initVector, o.Type, o.Tier, o.StorageMeta}

	return sql, args
}

func (o *Object) GetUpdateMetaSql() (string, []interface{}) {
	version := math.MaxUint64 - uint64(o.LastModified)
	attrs, _ := json.Marshal(o.CustomAttributes)
	sql := "update objects set customattributes =? where bucketname=? and name=? and version=?"
	args := []interface{}{attrs, o.BucketName, o.ObjectKey, version}
	return sql, args

}
