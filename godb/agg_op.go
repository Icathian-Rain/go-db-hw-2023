package godb

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group.
	newAggState []AggState

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Constructor for an aggregator with a group-by
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Constructor for an aggregator with no group-by
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

// Return a TupleDescriptor for this aggregation. If the aggregator has no group-by, the
// returned descriptor should contain the union of the fields in the descriptors of the
// aggregation states. If the aggregator has a group-by, the returned descriptor will
// additionally start with the group-by fields, and then the aggregation states descriptors
// like that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType
// HINT: use the merge function you implemented for TupleDesc in lab1 to merge the two TupleDescs
func (a *Aggregator) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// 构造fields
	fields := make([]FieldType, 0)
	// 若有分组依据，则将分组依据加入fields
	if a.groupByFields != nil {
		for _, fld := range a.groupByFields {
			fields = append(fields, fld.GetExprType())
		}
	}
	// 然后将聚合状态的fields加入fields
	td := TupleDesc{fields}
	for _, aggState := range a.newAggState {
		td.merge(aggState.GetTupleDesc())
	}
	return &td // TODO change me
}

// Aggregate operator implementation: This function should iterate over the results of
// the aggregate. The aggregate should be the result of aggregating each group's tuples
// and the iterator should iterate through each group's result. In the case where there
// is no group-by, the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
func (a *Aggregator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// the child iterator
	childIter, err := a.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}

	}
	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil"}
			}
			newAggState = append(newAggState, copy)
		}

		aggState[DefaultGroup] = &newAggState
	}
	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() (*Tuple, error)
	return func() (*Tuple, error) {
		// iterates thru all child tuples
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
				for i := 0; i < len(a.newAggState); i++ {
					(*aggState[DefaultGroup])[i].AddTuple(t)
				}
			} else { // adds tuple to the aggregation with grouping
				keygenTup, err := extractGroupByKeyTuple(a, t)
				if err != nil {
					return nil, err
				}

				key := keygenTup.tupleKey()
				if aggState[key] == nil {
					asNew := make([]AggState, len(a.newAggState))
					aggState[key] = &asNew
					groupByList = append(groupByList, keygenTup)
				}

				addTupleToGrpAggState(a, t, aggState[key])
			}
		}

		if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
			if a.groupByFields == nil {
				var tup *Tuple
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					tup = joinTuples(tup, newTup)
				}
				finalizedIter = func() (*Tuple, error) { return nil, nil }
				return tup, nil
			} else {
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iteror, return a tuple that identifies t's group.
// The returned tuple should contain the fields from the groupByFields list
// passed into the aggregator constructor.  The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
// If there is any error during expression evaluation, return the error.
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	// TODO: some code goes here
	// 构造指示t所属分组的keygenTup
	keygenTup := &Tuple{TupleDesc{[]FieldType{}}, []DBValue{}, nil}
	// 遍历分组依据，将分组依据加入keygenTup
	for _, expr := range a.groupByFields {
		// 计算出该分组依据对应的值
		v, err := expr.EvalExpr(t)
		if err != nil {
			return nil, err
		}
		// 将该分组依据加入keygenTup
		keygenTup = joinTuples(keygenTup, &Tuple{TupleDesc{[]FieldType{expr.GetExprType()}}, []DBValue{v}, nil})
	}
	// 返回keygenTup
	return keygenTup, nil // TODO change me
}

// Given a tuple t from child and (a pointer to) the array of partially computed aggregates
// grpAggState, add t into all partial aggregations using the [AggState AddTuple] method.
// If any of the array elements is of grpAggState is null (i.e., because this is the first
// invocation of this method, create a new aggState using aggState.Copy() on appropriate
// element of the a.newAggState field and add the new aggState to grpAggState.
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	// TODO: some code goes here
	// 遍历聚合状态
	for i, aggState := range *grpAggState {
		// 若聚合状态为空，则创建新的聚合状态
		if aggState == nil {
			copy := a.newAggState[i].Copy()
			if copy == nil {
				return
			}
			(*grpAggState)[i] = copy
		}
		// 将元组加入聚合状态中
		(*grpAggState)[i].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned tuples should
// be structured according to the TupleDesc returned from the Descriptor() method.
// HINT: you can call [aggState.Finalize()] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState tuples using the
// joinTuples function in tuple.go you wrote in lab 1.
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() (*Tuple, error) {
	// 当前组序号
	curGbyTuple := 0 // "captured" counter to track the current tuple we are iterating over
	return func() (*Tuple, error) {
		// TODO: some code goes here
		// 若最后一组，则返回
		if curGbyTuple >= len(groupByList) {
			return nil, nil
		}
		// 构造结果元组
		var retTuple *Tuple = &Tuple{TupleDesc{[]FieldType{}}, []DBValue{}, nil}
		// 首先将分组依据加入结果元组
		retTuple = joinTuples(retTuple, groupByList[curGbyTuple])
		// 获取该组的Key值
		key := groupByList[curGbyTuple].tupleKey()
		// 获取该组的聚合状态列表
		grpAggState := *aggState[key]
		// 遍历聚合状态列表，将结果添加到结果元组中
		for i := 0; i < len(a.newAggState); i++ {
			newTup := grpAggState[i].Finalize()
			retTuple = joinTuples(retTuple, newTup)
		}
		// 组ID++
		curGbyTuple++
		return retTuple, nil // TODO change me
	}
}
