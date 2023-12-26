package godb

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	// TODO: some code goes here
	pages    map[uint64]*Page // 用于存储页面, key为pageKey(FileName, PageNo)
	numPages int              // BufferPool的容量
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	pages := make(map[uint64]*Page, numPages)
	return &BufferPool{pages: pages, numPages: numPages}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	// 遍历所有页面，将页面写入磁盘
	for _, page := range bp.pages {
		// 如果页面不是空的，就写入磁盘
		if page != nil {
			file := (*page).getFile()
			(*file).flushPage(page)
		}
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// TODO: some code goes here
	hpfile := file.(*HeapFile)
	key := hpfile.pageKey(pageNo).(uint64)
	if bp.pages[key] != nil {
		return bp.pages[key], nil
	} else {
		// 判断buffer pool是否已满
		if len(bp.pages) >= bp.numPages {
			// 若已满，遍历所有页面，找到一个不是脏的页面，驱逐
			flag := false
			for _, page := range bp.pages {
				// 如果页面不是脏的，就可以驱逐
				if page != nil && !(*page).isDirty() {
					hpfile.flushPage(page)
					delete(bp.pages, hpfile.pageKey(pageNo).(uint64))
					flag = true
					break
				}
			}
			// 如果所有页面都是脏的，就报错
			if !flag {
				return nil, GoDBError{
					code:      PageFullError,
					errString: "buffer pool is full",
				}
			}
		}
		// 读取页面
		page, err := hpfile.readPage(pageNo)
		if err != nil {
			return nil, err
		}
		bp.pages[key] = page
		return page, nil
	}
}
