package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile     // 删除的文件
	child      Operator   // 输入的操作符
	desc       *TupleDesc // 描述符
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	// 返回一个删除操作符
	return &DeleteOp{deleteFile, child, &TupleDesc{
		Fields: []FieldType{
			{Fname: "count", Ftype: IntType},
		},
	}}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// 返回一个描述符，包含一个字段，名为count，类型为int，统计删除的数量
	return i.desc

}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 返回一个迭代器
	iter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
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
			// 删除
			err = dop.deleteFile.deleteTuple(tuple, tid)
			if err != nil {
				return nil, err
			}
			// 计数
			count++
		}
		// 返回一个只有一个字段的元组，字段为count，类型为int
		return &Tuple{
			Desc: *dop.desc,
			Fields: []DBValue{
				IntField{int64(count)},
			},
		}, nil
	}, nil
}
