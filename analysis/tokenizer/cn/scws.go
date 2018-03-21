package cn

/*
// 引入头文件有几种方法
// (1) 直接在.go文件中使用CFLAGS指定路径
// (2) shell的环境变量C_INCLUDE_PATH
// (3) shell的环境变量CGO_CFLAGS也可以
// 引入库文件同样的方法,设置LDFLAGS或者LIBRARY_PATH或者CGO_CFLAGS
//
// 这里假设通过C_INCLUDE_PATH和LIBRARY_PATH能找到scws的头文件和库文件

char * CharOff2String(char* text,int off) {
    return text+off;
}

char * CharArray2String(char text[3]) {
    return &text[0];
}

#include <stdlib.h>
#include <string.h>
#include "scws/scws.h"
#cgo LDFLAGS : -lscws
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

const (
	// SetMulti mode
	SCWS_MULTI_SHORT   = C.SCWS_MULTI_SHORT
	SCWS_MULTI_DUALITY = C.SCWS_MULTI_DUALITY
	SCWS_MULTI_ZMAIN   = C.SCWS_MULTI_ZMAIN
	SCWS_MULTI_ZALL    = C.SCWS_MULTI_ZALL

	// SetDict/AddDict mode
	SCWS_XDICT_TXT = C.SCWS_XDICT_TXT
	SCWS_XDICT_XDB = C.SCWS_XDICT_XDB
	SCWS_XDICT_MEM = C.SCWS_XDICT_MEM
)

// 分词结果
type ScwsRes struct {
	Term   string  //分词的结果
	Attr   string  //词性
	Idf    float64 //idf值
	Start  int
	End    int
	Length int
}

// Scws是封装好的切词服务.提供一个简单的切词接口.
type Scws struct {
	root C.scws_t

	forkScws chan C.scws_t
}

// 设定当前 scws 所使用的字符集.
// 参数cs 新指定的字符集.若无此调用则系统缺省使用gbk,还支持utf8,指定字符集时参数的大小写不敏感
// 错误若指定的字符集不存在,则会自动使用gbk 字符集替代.
func (this *Scws) SetCharset(cs string) {
	ctext := C.CString(cs)
	defer C.free(unsafe.Pointer(ctext))
	C.scws_set_charset(this.root, ctext)
}

// 添加词典文件到当前scws 对象
// 参数fpath 词典的文件路径,词典格式是XDB或XT 格式。
// 参数mode有3种值,分别为预定义的：
// SCWS_XDICT_TXT 表示要读取的词典文件是文本格式，可以和后2项结合用
// SCWS_XDICT_XDB 表示直接读取 xdb 文件
// SCWS_XDICT_MEM 表示将 xdb 文件全部加载到内存中，以 XTree 结构存放，可用异或结合另外2个使用。
// 具体用哪种方式需要根据自己的实际应用来决定。当使用本库做为守护进程时推荐使用 mem 方式， 当只是嵌入调用时应该使用 xdb 方式，将 xdb 文件加载进内存不仅占用了比较多的内存， 而且也需要一定的时间（35万条数据约需要0.3~0.5秒左右）
func (this *Scws) AddDict(fPath string, mode int) error {
	ctext := C.CString(fPath)
	defer C.free(unsafe.Pointer(ctext))
	ret := int(C.scws_add_dict(this.root, ctext, C.int(mode)))
	if ret != 0 {
		return errors.New(fmt.Sprintf("Add Dict [%s] Fail", fPath))
	}
	return nil
}

// 清除并设定当前scws 操作所有的词典文件
func (this *Scws) SetDict(fPath string, mode int) error {
	ctext := C.CString(fPath)
	defer C.free(unsafe.Pointer(ctext))
	ret := int(C.scws_set_dict(this.root, ctext, C.int(mode)))
	if ret != 0 {
		return errors.New(fmt.Sprintf("Set Dict [%s] Fail", fPath))
	}
	return nil
}

// 设定规则集文件
func (this *Scws) SetRule(fPath string) error {
	ctext := C.CString(fPath)
	defer C.free(unsafe.Pointer(ctext))
	C.scws_set_rule(this.root, ctext)
	if this.root.r == nil {
		return errors.New(fmt.Sprintf("Set Rule [%s] Fail", fPath))
	}
	return nil
}

// 设定分词结果是否忽略所有的标点等特殊符号(不会忽略\r和\n)
// 参数yes 1 表示忽略,0 表示不忽略,缺省情况为不忽略
func (this *Scws) SetIgnore(yes int) {
	C.scws_set_ignore(this.root, C.int(yes))
}

// 设定分词执行时是否执行针对长词复合切分。（例：“中国人”分为“中国”、“人”、“中国人”）。
// 参数mode复合分词法的级别,缺省不复合分词.取值由下面几个常量异或组合:
// SCWS_MULTI_SHORT 短词
// SCWS_MULTI_DUALITY 二元（将相邻的2个单字组合成一个词）
// SCWS_MULTI_ZMAIN 重要单字
// SCWS_MULTI_ZALL 全部单字
func (this *Scws) SetMulti(mode int) {
	C.scws_set_multi(this.root, C.int(mode))
}

// 设定是否将闲散文字自动以二字分词法聚合。
// 参数yes 如果为1 表示执行二分聚合,0 表示不处理,缺省为0
func (this *Scws) SetDuality(yes int) {
	C.scws_set_duality(this.root, C.int(yes))
}

// 未实现,若有需要加上
// scws_get_tops
// scws_free_tops
// scws_has_word
// scws_get_words

func (this *Scws) Segment(text string) ([]ScwsRes, error) {
	if this.forkScws == nil {
		return nil, errors.New("必须在非并发情况下调用一次Scws.Init")
	}
	// 分词结果数组
	scwsResult := make([]ScwsRes, 0)

	// 从队列取一个用
	tmpScws := <-this.forkScws

	ctext := C.CString(text)
	defer C.free(unsafe.Pointer(ctext))

	C.scws_send_text(tmpScws, ctext, C.int(len(text)))
	res := C.scws_get_result(tmpScws)
	for res != nil {

		cur := res
		for cur != nil {
			attr := (*C.char)(unsafe.Pointer(&cur.attr[0]))
			scwsResult = append(scwsResult, ScwsRes{
				Term:   C.GoStringN(C.CharOff2String(ctext, cur.off), C.int(cur.len)),
				Idf:    float64(cur.idf),
				Start:  int(cur.off),
				End:    int(cur.off) + int(cur.len),
				Length: int(cur.len),
				Attr:   C.GoStringN(attr, C.int(C.strlen(attr)))})
			cur = cur.next
		}

		// 释放这个结果,获取下个结果
		C.scws_free_result(res)
		res = C.scws_get_result(tmpScws)
	}

	// 用完放入队列
	this.forkScws <- tmpScws

	return scwsResult, nil
}

// 释放Scws的全部资源.
func (this *Scws) Free() error {
	if this.forkScws != nil {
		close(this.forkScws)
		for s := range this.forkScws {
			C.scws_free(s)
		}
	}

	if this.root != nil {
		C.scws_free(this.root)
		this.root = nil
	}

	return nil
}

// 内部复制多个scws实例,并发调用Segment的时候可以并发切词.
// 否则Segment也能正常使用,只是所有切词都是串行执行.
func (this *Scws) Init(count int) error {
	if this.forkScws != nil {
		return errors.New("Scws.Init只允许调用一次")
	}
	if count < 1 {
		return errors.New("不能少于1个实例")
	}
	this.forkScws = make(chan C.scws_t, count)

	for i := 0; i < count; i++ {
		tmp := C.scws_fork(this.root)
		if tmp != nil {
			this.forkScws <- tmp
		}
	}
	if len(this.forkScws) != count {
		return errors.New("内存不足导致fork数量不符合预期")
	}
	return nil
}

// Scws构造函数
func NewScws() *Scws {
	s := &Scws{}
	s.root = C.scws_new()
	s.forkScws = nil
	return s
}
