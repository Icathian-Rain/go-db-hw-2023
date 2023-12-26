package godb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	pageNo   int        // 页号
	tuples   []*Tuple   // 页面上的元组, 若为nil代表空闲
	td       *TupleDesc // 元组描述
	numSlots int32      // 页面上的总槽位数
	numUsed  int32      // 页面上已经使用的槽位数
	dirty    bool       // 是否脏页
	file     DBFile     // page对应的DBFile
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	size := 0
	// 计算单个元组大小
	for _, v := range desc.Fields {
		switch v.Ftype {
		case IntType:
			// 若为int64, 则size为8
			size += int(unsafe.Sizeof(int64(0)))
		case StringType:
			// 若为字符串，则为StringLength大小
			size += StringLength * int(unsafe.Sizeof(byte('a')))
		}
	}
	// 计算槽位数
	numSlots := int32((PageSize - 8) / size)
	return &heapPage{
		pageNo:   pageNo,
		tuples:   make([]*Tuple, numSlots),
		td:       desc,
		numSlots: numSlots,
		numUsed:  0,
		dirty:    false,
		file:     f,
	} //replace me
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return int(h.numSlots) //replace me
}

func (h *heapPage) getNumEmptySlots() int {
	return int(h.numSlots - h.numUsed)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	// 判断是否为符合的元组
	if h.td.equals(&t.Desc) {
		// 判断是否有空位
		if h.numUsed < h.numSlots {
			// 寻找空位
			for i, v := range h.tuples {
				// 若为空，则插入
				if v == nil {
					t.Rid = RecordID{
						PageNo: h.pageNo,
						SlotNo: i,
					}
					h.tuples[i] = t
					h.numUsed++
					// 设置
					h.dirty = true
					return t.Rid, nil
				}
			}
			// 若没有空位， 返回错误
			return 0, GoDBError{code: PageFullError, errString: "no free slots"}
		}
		// 若没有空位，返回错误
		return 0, GoDBError{code: PageFullError, errString: "no free slots"}
	}
	// 若类型不符合，返回错误
	return 0, GoDBError{code: TypeMismatchError, errString: "tuple's desc doesn't match"} //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	// 获取槽位号
	SlotNo := rid.(RecordID).SlotNo
	// 判断槽位号是否合法
	if int32(SlotNo) < h.numSlots {
		if h.tuples[SlotNo] != nil {
			h.tuples[SlotNo] = nil
			h.numUsed--
			h.dirty = true
			return nil
		}
		return GoDBError{code: TupleNotFoundError, errString: "tuple not found"}
	}
	return GoDBError{code: TupleNotFoundError, errString: "tuple Numer over"} //replace me
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	return &p.file //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	buf := new(bytes.Buffer)
	// 写入页头
	// 写入槽位数
	err := binary.Write(buf, binary.LittleEndian, h.numSlots)
	if err != nil {
		return nil, err
	}
	// 写入已使用的槽位数
	err = binary.Write(buf, binary.LittleEndian, h.numUsed)
	if err != nil {
		return nil, err
	}
	// 写入元组
	for _, v := range h.tuples {
		// 若不为空，则写入
		if v != nil {
			err = v.writeTo(buf)
			if err != nil {
				return nil, err
			}
		}
	}
	// 若还有空间，则填充
	paddingLen := PageSize - buf.Len()
	paddingBytes := make([]byte, paddingLen)
	_, err = buf.Write(paddingBytes)
	if err != nil {
		return nil, err
	}
	return buf, nil //replace me
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	// 读取页头
	// 读取槽位数
	binary.Read(buf, binary.LittleEndian, &h.numSlots)
	// 读取已使用的槽位数
	binary.Read(buf, binary.LittleEndian, &h.numUsed)
	// 读取元组
	for i := 0; i < int(h.numUsed); i++ {
		// 读取元组
		t, err := readTupleFrom(buf, h.td)
		if err != nil {
			return err
		}
		// 写入tuples
		h.tuples[i] = t
		// 更新tuples的Rid
		h.tuples[i].Rid = RecordID{
			PageNo: h.pageNo,
			SlotNo: i,
		}
	}
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	// index作为迭代
	index := 0
	return func() (*Tuple, error) {
		// 若index小于槽位数且tuples[index]为空，则index++
		// 直到index大于槽位数或tuples[index]不为空
		for index < int(p.numSlots) && p.tuples[index] == nil {
			index++
		}
		// 若index小于槽位数，则返回tuples[index]
		if index < int(p.numSlots) {
			index++
			return p.tuples[index], nil
		}
		return nil, nil
	} //replace me
}
