package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	//add additional fields here
	// TODO: some code goes here
	distinct bool       // 是否去重
	desc     *TupleDesc // 描述符
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	// 若selectFields和outputNames长度不一致，返回错误
	if len(selectFields) != len(outputNames) {
		return nil, GoDBError{ParseError, "selectFields and outputNames must be same length"}
	}
	// 构建TupleDesc
	fields := make([]FieldType, len(selectFields))
	// 遍历selectFields，获取每个field的类型
	// 将field的name修改为outputNames中对应的name
	for i, field := range selectFields {
		fields[i] = field.GetExprType()
		fields[i].Fname = outputNames[i]
	}
	return &Project{selectFields, outputNames, child, distinct, &TupleDesc{fields}}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return p.desc

}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 获取child的迭代器
	iter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
	var set = make(map[any]bool)
	return func() (*Tuple, error) {
		// 遍历child的迭代器
		for {
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			// 若tuple为nil，说明已经遍历完毕
			if tuple == nil {
				break
			}
			// 提取出fileds
			fields := make([]DBValue, len(p.selectFields))
			for i, field := range p.selectFields {
				fields[i], err = field.EvalExpr(tuple)
				if err != nil {
					return nil, nil
				}
			}
			// 构造返回tuple
			retTuple := &Tuple{
				Desc:   *p.desc,
				Fields: fields,
			}
			key := retTuple.tupleKey()
			// 查重
			_, ok := set[key]
			// 若不需要去重或者没有重复，返回tuple
			if !p.distinct || !ok {
				set[key] = true
				return retTuple, nil
			}
		}
		return nil, nil
	}, nil

}
