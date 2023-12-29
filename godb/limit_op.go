package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// TODO: some code goes here
	return &LimitOp{child, lim} //replace me
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor() //replace me

}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 获取limit的值
	nValue, err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, nil
	}
	n := nValue.(IntField).Value
	// 获取child的迭代器
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	// 返回一个函数，该函数每次调用都会返回一个tuple
	i := 0
	return func() (*Tuple, error) {
		// 如果i大于等于n，说明已经返回了n个tuple，返回nil
		if i >= int(n) {
			return nil, nil
		}
		// 否则，调用child的迭代器，返回一个tuple
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		i++
		return tup, nil
	}, nil

}
