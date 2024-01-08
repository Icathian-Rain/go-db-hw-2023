package godb

import (
	"context"
	"fmt"
	"sync"
	"time"
)

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
	pages    map[uint64]*Page                    // 用于存储页面, key为pageKey(FileName, PageNo) map[uint64]*Page
	numPages int                                 // BufferPool的容量
	mutexMap map[uint64]*sync.RWMutex            // 用于存储页面锁 map[uint64]*sync.RWMutex
	tidMap   map[TransactionID]map[uint64]RWPerm // 用于存储tid持有的锁 map[TransactionID]map[uint64]RWPerm
	mutex    *sync.Mutex                         // 用于保护pages和mutexMap
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	// TODO: some code goes here
	// 初始化BufferPool
	pages := make(map[uint64]*Page)
	mutexMap := make(map[uint64]*sync.RWMutex)
	tidMap := make(map[TransactionID]map[uint64]RWPerm)
	return &BufferPool{pages, numPages, mutexMap, tidMap, &sync.Mutex{}}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	// 遍历所有页面，将页面写入磁盘
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	for _, page := range bp.pages {
		// 如果页面不是空的，就写入磁盘
		if page != nil {
			file := (*page).getFile()
			(*file).flushPage(page)
		}
	}
	bp.pages = make(map[uint64]*Page)
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
	// 遍历页面，丢弃所有tid的页面
	fmt.Println("abort transaction", tid)
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	mutexMap, ok := bp.tidMap[tid]
	if !ok {
		return
	}
	for key, v := range mutexMap {
		if v == ReadPerm {
			bp.mutexMap[key].RUnlock()
		} else {
			delete(bp.pages, key)
			bp.mutexMap[key].Unlock()
		}
	}
	delete(bp.tidMap, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	// 遍历所有页面，将页面写入磁盘
	fmt.Println("commit transaction", tid)
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	// 释放tid的锁
	mutexMap, ok := bp.tidMap[tid]
	if !ok {
		return
	}
	for key, v := range mutexMap {
		if v == ReadPerm {
			bp.mutexMap[key].RUnlock()
		} else {
			page := bp.pages[key]
			if page != nil {
				file := (*page).getFile()
				(*file).flushPage(page)
			}
			delete(bp.pages, key)
			bp.mutexMap[key].Unlock()
		}
	}
	delete(bp.tidMap, tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	// 添加tid到tidMap中
	bp.mutex.Lock()
	bp.tidMap[tid] = make(map[uint64]RWPerm)
	bp.mutex.Unlock()
	return nil
}

func (bp *BufferPool) HasTransaction(tid TransactionID) bool {
	if _, ok := bp.tidMap[tid]; ok {
		return true
	} else {
		return false
	}
}

func lockTimeout(lock *sync.RWMutex, perm RWPerm, timeOut int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeOut)*time.Millisecond)
	defer cancel()

	var lockAcquired bool

	done := make(chan struct{})

	go func() {
		defer close(done)

		if perm == ReadPerm {
			lock.RLock()
		} else {
			lock.Lock()
		}

		lockAcquired = true
	}()

	select {
	case <-done:
		// 在超时时间内获取到锁
	case <-ctx.Done():
		// 超时
		fmt.Println("lock timeout")
		if !lockAcquired {
			// 如果没有获取到锁，释放可能已经获取到的锁
			if perm == ReadPerm {
				lock.RUnlock()
			} else {
				lock.Unlock()
			}
		}
	}

	return lockAcquired
}

func (bp *BufferPool) lockPage(pageKey uint64, tid TransactionID, perm RWPerm, timeOut int) bool {
	bp.mutex.Lock()
	// 获取该页面的锁
	lock, ok := bp.mutexMap[pageKey]
	if !ok {
		lock = &sync.RWMutex{}
		bp.mutexMap[pageKey] = lock
	}
	// 判断当前的tid是否已经持有该页面的锁
	curPerm, ok := bp.tidMap[tid][pageKey]
	bp.mutex.Unlock()
	if !ok {
		// 如果当前的tid没有持有该页面的锁，就加锁
		// 如果超时，就返回false
		if !lockTimeout(lock, perm, timeOut) {
			return false
		}
		bp.mutex.Lock()
		bp.tidMap[tid][pageKey] = perm
		bp.mutex.Unlock()
	} else if curPerm == ReadPerm && perm == WritePerm {
		// 如果当前的权限是Read，但是需要的权限是Write，就升级锁
		lock.RUnlock()
		bp.mutex.Lock()
		delete(bp.tidMap[tid], pageKey)
		bp.mutex.Unlock()
		if !lockTimeout(lock, perm, timeOut) {
			return false
		}
		bp.mutex.Lock()
		bp.tidMap[tid][pageKey] = perm
		bp.mutex.Unlock()
	}
	return true
}

func (bp *BufferPool) unlockPage(pageKey uint64, tid TransactionID, perm RWPerm) (bool, error) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	// 获取该页面的锁
	lock, ok := bp.mutexMap[pageKey]
	if !ok {
		return false, GoDBError{
			code:      0,
			errString: "page lock not found",
		}
	}
	// 判断当前的tid是否已经持有该页面的锁
	curPerm, ok := bp.tidMap[tid][pageKey]
	if !ok {
		return false, GoDBError{
			code:      0,
			errString: "page lock not found",
		}
	}
	// 释放锁
	if curPerm == perm {
		if perm == ReadPerm {
			lock.RUnlock()
		} else {
			lock.Unlock()
		}
		// 从tidMap中删除tid
		delete(bp.tidMap[tid], pageKey)
	}

	return true, nil
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
	if !bp.lockPage(key, tid, perm, 100) {
		return nil, GoDBError{
			code:      DeadlockError,
			errString: "lock timeout",
		}
	}
	// 读取页面
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	page, ok := bp.pages[key]
	if ok {
		return page, nil
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
