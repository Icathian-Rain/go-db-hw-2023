package godb

import "sort"

// TODO: some code goes here
type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	// 若orderByFields和ascending长度不一致，返回错误
	if len(orderByFields) != len(ascending) {
		return nil, GoDBError{code: 0, errString: "length of orderByFields and ascending not equal"}
	}
	return &OrderBy{orderByFields, child, ascending}, nil //replace me

}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor() //replace me
}

type Data struct {
	tuples []*Tuple
	o      *OrderBy
}

func (d *Data) Len() int {
	return len(d.tuples)
}

func (d *Data) Swap(i, j int) {
	d.tuples[i], d.tuples[j] = d.tuples[j], d.tuples[i]
}

func (d *Data) Less(i, j int) bool {
	for t, v := range d.o.orderBy {
		t1 := d.tuples[i]
		t2 := d.tuples[j]
		res, err := t1.compareField(t2, v)
		if err != nil {
			panic(err)
		}
		if res == OrderedEqual {
			continue
		}
		if d.o.ascending[t] {
			return res == OrderedLessThan
		} else {
			return res == OrderedGreaterThan
		}
	}
	return true
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 构造一个Data结构体，包含tuples和OrderBy
	data := &Data{make([]*Tuple, 0), o}
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	// 遍历child的所有tuple，将其加入到data.tuples中
	for {
		t, err := iter()
		if err != nil {
			return nil, err
		}
		if t == nil {
			break
		}
		data.tuples = append(data.tuples, t)
	}
	// 使用sort.Sort()对data.tuples进行排序
	sort.Sort(data)
	i := 0
	return func() (*Tuple, error) {
		if i >= len(data.tuples) {
			return nil, nil
		}
		// 返回排序后的tuple
		t := data.tuples[i]
		i++
		return t, nil
	}, nil //replace me
}
