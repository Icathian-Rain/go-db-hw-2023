package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile     // 插入的文件
	child      Operator   // 被插入的操作符
	desc       *TupleDesc // 描述符
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	// 返回一个插入操作符
	return &InsertOp{insertFile, child, &TupleDesc{
		Fields: []FieldType{
			{Fname: "count", Ftype: IntType},
		},
	}}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// 返回一个描述符，包含一个字段，名为count，类型为int，统计插入的数量
	return i.desc
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 返回一个迭代器
	iter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
	// 计数
	count := 0
	return func() (*Tuple, error) {
		for {
			// 迭代
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				break
			}
			// 插入
			err = iop.insertFile.insertTuple(tuple, tid)
			if err != nil {
				return nil, err
			}
			// 计数
			count++
		}
		// 返回结果元组，包含插入的数量
		return &Tuple{
			Desc: *iop.desc,
			Fields: []DBValue{
				IntField{int64(count)},
			},
		}, nil
	}, nil
}
