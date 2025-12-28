package block

import (
	"github.com/xmh1011/go-kv/engine/lsm/kv"
)

// Block 定义了 SSTable 中数据块的通用接口
type Block interface {
	EncodeTo(w interface{}) error
	DecodeFrom(r interface{}) error
}

// Entry 定义了 Block 中的条目
type Entry struct {
	Key   kv.Key
	Value kv.Value
}
