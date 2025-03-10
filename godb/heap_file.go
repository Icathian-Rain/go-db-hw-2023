package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/mitchellh/hashstructure/v2"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	bufPool    *BufferPool // buffer pool
	sync.Mutex             // mutex
	fromFile   string      // file name
	td         *TupleDesc  // tuple descriptor
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	// 创建或打开文件
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return &HeapFile{
		fromFile: fromFile,
		td:       td,
		bufPool:  bp,
	}, nil //replace me
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	// 获取文件大小
	FileInfo, err := os.Stat(f.fromFile)
	if err != nil {
		return 0
	}
	fileSize := FileInfo.Size()
	// 计算页数
	return int(fileSize) / PageSize //replace me
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for j := 0; j < f.NumPages(); j++ {
			pg, err := bp.GetPage(f, j, tid, ReadPerm)
			if pg == nil || err != nil {
				fmt.Println("page nil or error", err)
				break
			}
			if (*pg).isDirty() {
				(*f).flushPage(pg)
				(*pg).setDirty(false)
			}

		}

		//commit frequently, to avoid all pages in BP being full
		//todo fix
		bp.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// TODO: some code goes here
	// 打开文件
	file, _ := os.Open(f.fromFile)
	defer file.Close()
	buf := make([]byte, PageSize)
	// 计算偏移量
	offset := PageSize * pageNo
	// 读取文件
	_, err := file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}
	// 根据数据库内容初始化page
	hp := newHeapPage(f.td, pageNo, f)
	err = hp.initFromBuffer(bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	var page Page = hp
	// 返回page
	return &page, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	if !f.bufPool.HasTransaction(tid) {
		return GoDBError{code: 0, errString: "transaction not found"}
	}
	// TODO: some code goes here
	// 从现有的page中寻找空slot
	for i := 0; i < f.NumPages(); i++ {
		// 获取page
		page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
		// 若获取失败，说明该页面被锁住，跳过
		if err != nil {
			continue
		}
		hp := (*page).(*heapPage)
		// 如果有空slot，插入tuple
		if hp.getNumEmptySlots() > 0 {
			// 插入tuple
			page, err := f.bufPool.GetPage(f, i, tid, WritePerm)
			if err != nil {
				continue
			}
			_, err = (*page).(*heapPage).insertTuple(t)
			if err != nil {
				return err
			}
			// set dirty
			(*page).setDirty(true)
			return nil
		}
		f.bufPool.unlockPage(f.pageKey(i).(uint64), tid, ReadPerm)
	}
	// no empty slots found, create new page
	pageNo := f.NumPages()
	hp := newHeapPage(f.td, pageNo, f)
	// 刷新page
	// write page to end of file
	var page Page = hp
	// 刷新page
	f.flushPage(&page)
	new_page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	// add tuple to new page
	_, err = (*new_page).(*heapPage).insertTuple(t)
	if err != nil {
		return err
	}
	// set dirty
	(*new_page).setDirty(true)

	return nil //replace me
}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	if !f.bufPool.HasTransaction(tid) {
		return GoDBError{code: 0, errString: "transaction not found"}
	}
	// 获取page
	pageID := t.Rid.(RecordID).PageNo
	page, err := f.bufPool.GetPage(f, int(pageID), tid, WritePerm)
	if err != nil {
		f.bufPool.AbortTransaction(tid)
		return err
	}
	// 删除tuple
	err = (*page).(*heapPage).deleteTuple(t.Rid)
	if err != nil {
		return err
	}
	// set dirty
	(*page).setDirty(true)
	return nil //replace me
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	// TODO: some code goes here
	// 打开文件
	file, _ := os.OpenFile(f.fromFile, os.O_RDWR|os.O_CREATE, 0666)
	defer file.Close()
	pageNo := (*p).(*heapPage).pageNo
	// 计算偏移量
	offset := pageNo * PageSize
	// 将page转换为buffer
	buf, err := (*p).(*heapPage).toBuffer()
	if err != nil {
		return err
	}
	// 写入文件
	number_bytes, err := file.WriteAt(buf.Bytes(), int64(offset))
	if err != nil {
		return err
	}
	_ = number_bytes
	return nil //replace me
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.td //replace me
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	if !f.bufPool.HasTransaction(tid) {
		return nil, GoDBError{code: 0, errString: "transaction not found"}
	}
	// 迭代page
	pageNo := 0
	// page迭代tuple
	var iter func() (*Tuple, error) = nil
	// TODO: some code goes here
	return func() (*Tuple, error) {
		// 遍历page
		for pageNo < f.NumPages() {
			// 获取page
			page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
			if err != nil {
				f.bufPool.AbortTransaction(tid)
				return nil, err
			}
			hp := (*page).(*heapPage)
			// 获取page的迭代器
			if iter == nil {
				iter = hp.tupleIter()
			}
			for {
				// 获取tuple
				t, err := iter()
				if err != nil {
					return nil, err
				}
				if t == nil {
					// 若tuple为空，则pageNo++, iter置空
					break
				}
				return t, nil
			}
			// 上一个页面迭代完毕
			pageNo++
			iter = nil
		}
		return nil, nil
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	HeapHash := heapHash{
		FileName: f.fromFile,
		PageNo:   pgNo,
	}
	hash, _ := hashstructure.Hash(HeapHash, hashstructure.FormatV2, nil)
	return hash //replace me
}
