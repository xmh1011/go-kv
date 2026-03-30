// 定义 kv 和 存储方式
// 采用小端存储，使用长度前缀编码
/*
┌────────────┬──────────┬──────────────┬────────────┐
│ key length │ key data │ value length │ value data │
└────────────┴──────────┴──────────────┴────────────┘
*/

package kv

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xmh1011/go-kv/pkg/log"
)

type (
	Key   string
	Value []byte
)

type KeyValuePair struct {
	Key   Key
	Value Value
}

const deletedValueStr = "～DELETED～"

var DeletedValue = Value(deletedValueStr)

func (p *KeyValuePair) Copy() *KeyValuePair {
	return &KeyValuePair{
		Key:   p.Key,
		Value: p.Value,
	}
}

func (p *KeyValuePair) IsDeleted() bool {
	// 判断 Value 是否为删除标记
	return p.Value != nil && string(p.Value) == deletedValueStr
}

// EncodeTo 使用4字节小端编码，单次写入优化
func (p *KeyValuePair) EncodeTo(w io.Writer) error {
	// 预分配 buffer: 4(keyLen) + key + 4(valLen) + value
	keyLen := len(p.Key)
	valLen := len(p.Value)
	buf := make([]byte, 4+keyLen+4+valLen)

	// 编码 key 长度（4字节小端）
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keyLen))
	// 编码 key 数据
	copy(buf[4:4+keyLen], p.Key)
	// 编码 value 长度（4字节小端）
	binary.LittleEndian.PutUint32(buf[4+keyLen:8+keyLen], uint32(valLen))
	// 编码 value 数据
	copy(buf[8+keyLen:], p.Value)

	// 单次写入
	if _, err := w.Write(buf); err != nil {
		log.Errorf("write key-value pair failed: %s", err)
		return fmt.Errorf("encode key-value pair: %w", err)
	}

	return nil
}

// DecodeFrom 使用4字节小端解码
func (p *KeyValuePair) DecodeFrom(r io.Reader) error {
	// 解码 key 长度（4字节小端）
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		log.Errorf("read key length failed: %s", err)
		return fmt.Errorf("decode key length: %w", err)
	}
	if keyLen > 1<<20 {
		return fmt.Errorf("invalid key length: %d", keyLen)
	}

	// 解码 key 数据
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		log.Errorf("read key failed: %s", err)
		return fmt.Errorf("decode key: %w", err)
	}
	p.Key = Key(key)

	// 解码 value 长度（4字节小端）
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		log.Errorf("read value length failed: %s", err)
		return fmt.Errorf("decode value length: %w", err)
	}
	if valLen > 1<<30 {
		return fmt.Errorf("invalid value length: %d", valLen)
	}

	// 解码 value 数据
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		log.Errorf("read value failed: %s", err)
		return fmt.Errorf("decode value: %w", err)
	}
	p.Value = val

	return nil
}

// EstimateSize 估算编码后大小
func (p *KeyValuePair) EstimateSize() uint64 {
	// 4字节 key 长度 + key 数据长度 + 4字节 value 长度 + value 数据长度 + 8字节 value offset
	return 4 + uint64(len(p.Key)) + 4 + uint64(len(p.Value)) + 8
}

// DecodeFrom 从 io.Reader 解码 Key（小端存储 + 4字节长度前缀）
func (k *Key) DecodeFrom(r io.Reader) (int64, error) {
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		log.Errorf("read key keyLen failed: %s", err)
		return 0, fmt.Errorf("decode key keyLen: %w", err)
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		log.Errorf("read key bytes failed: %s", err)
		return 0, fmt.Errorf("decode key bytes: %w", err)
	}

	*k = Key(keyBytes)
	return int64(4 + len(keyBytes)), nil
}

// EncodeTo 编码 Key（小端存储 + 4字节长度前缀），并返回写入的字节数
func (k *Key) EncodeTo(w io.Writer) (int64, error) {
	keyLen := len(*k)
	buf := make([]byte, 4+keyLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(keyLen))
	copy(buf[4:], *k)

	n, err := w.Write(buf)
	if err != nil {
		log.Errorf("write key failed: %s", err.Error())
		return int64(n), fmt.Errorf("encode key: %w", err)
	}
	return int64(n), nil
}

// EncodeTo 编码 Value（小端存储 + 4字节长度前缀）
func (v *Value) EncodeTo(w io.Writer) (int64, error) {
	valLen := len(*v)
	buf := make([]byte, 4+valLen)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(valLen))
	copy(buf[4:], *v)

	n, err := w.Write(buf)
	if err != nil {
		log.Errorf("write value failed: %s", err)
		return int64(n), fmt.Errorf("encode value: %w", err)
	}
	return int64(n), nil
}

// DecodeFrom 从 io.Reader 解码 Value（小端存储 + 4字节长度前缀）
func (v *Value) DecodeFrom(r io.Reader) error {
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		log.Errorf("read value length failed: %s", err)
		return fmt.Errorf("decode value length: %w", err)
	}

	if valLen > 1<<30 { // 1GB
		return fmt.Errorf("invalid value length: %d", valLen)
	}

	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		log.Errorf("read value bytes failed: %s", err)
		return fmt.Errorf("decode value: %w", err)
	}

	*v = val
	return nil
}
