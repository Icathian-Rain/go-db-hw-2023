package godb

import "os"

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	// TODO: some code goes here
	// 打开数据库文件
	lab1_bp := "lab1_bp.dat"
	// 如果文件存在，就删除
	if _, err := os.Stat(lab1_bp); err == nil {
		os.Remove(lab1_bp)
	}
	// 新建heapfile，bufferpool为100
	hpfile, err := NewHeapFile(lab1_bp, &td, NewBufferPool(100))
	if err != nil {
		return 0, err
	}
	// 打开csv文件，从csv文件中读取数据，写入heapfile
	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	err = hpfile.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return 0, err
	}
	tid := NewTID()
	// 开启事务
	hpfile.bufPool.BeginTransaction(tid)
	// 遍历heapfile，计算sum
	iter, err := hpfile.Iterator(tid)
	if err != nil {
		return 0, err
	}
	sum := 0
	for {
		tup, err := iter()
		if err != nil {
			return 0, err
		}
		if tup == nil {
			break
		}
		// 遍历tuple，找到sumField
		for i, field := range td.Fields {
			if field.Fname == sumField {
				sum += int(tup.Fields[i].(IntField).Value)
			}
		}
	}
	// 返回sum
	hpfile.bufPool.CommitTransaction(tid)
	return sum, nil // replace me
}
