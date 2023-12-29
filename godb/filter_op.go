package godb

import "golang.org/x/exp/constraints"

type Filter[T constraints.Ordered] struct {
	op     BoolOp          // 比较符号
	left   Expr            // 左边的表达式
	right  Expr            // 右边的表达式
	child  Operator        // HeapFile, 用来获取迭代器与TupleDescriptor
	getter func(DBValue) T // 获取值的函数
}

func intFilterGetter(v DBValue) int64 {
	intV := v.(IntField)
	return intV.Value
}

func stringFilterGetter(v DBValue) string {
	stringV := v.(StringField)
	return stringV.Value
}

// Constructor for a filter operator on ints
func NewIntFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[int64], error) {
	if constExpr.GetExprType().Ftype != IntType || field.GetExprType().Ftype != IntType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply int filter to non int-types"}
	}
	f, err := newFilter[int64](constExpr, op, field, child, intFilterGetter)
	return f, err
}

// Constructor for a filter operator on strings
func NewStringFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter[string], error) {
	if constExpr.GetExprType().Ftype != StringType || field.GetExprType().Ftype != StringType {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply string filter to non string-types"}
	}
	f, err := newFilter[string](constExpr, op, field, child, stringFilterGetter)
	return f, err
}

// Getter is a function that reads a value of the desired type
// from a field of a tuple
// This allows us to have a generic interface for filters that work
// with any ordered type
func newFilter[T constraints.Ordered](constExpr Expr, op BoolOp, field Expr, child Operator, getter func(DBValue) T) (*Filter[T], error) {
	return &Filter[T]{op, field, constExpr, child, getter}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter[T]) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over
// the results of the child iterator and return a tuple if it satisfies
// the predicate.
// HINT: you can use the evalPred function defined in types.go to compare two values
func (f *Filter[T]) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// 若左边表达式的类型不等于右边表达式的类型，则返回错误
	if f.left.GetExprType().Ftype != f.right.GetExprType().Ftype {
		return nil, GoDBError{IncompatibleTypesError, "cannot apply filter to non matching types"}
	}
	// 获取迭代器
	iter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	// 右表达式为定值，提前计算
	rightVal, err := f.right.EvalExpr(nil)
	if err != nil {
		return nil, err
	}
	// 迭代器为空，返回空
	if iter == nil {
		return nil, nil
	}
	// 返回迭代函数
	return func() (*Tuple, error) {
		for {
			// 获取下一个tuple
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				break
			}
			// 获取左右表达式的值
			leftVal, err := f.left.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}
			// 比较左右表达式的值
			if evalPred[T](f.getter(leftVal), f.getter(rightVal), f.op) {
				return tuple, nil
			}
		}
		return nil, nil
	}, nil
}
