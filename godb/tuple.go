package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	// TODO: some code goes here
	// 若 d1 和 d2 的长度不相等，则返回 false
	length := len(d1.Fields)
	if length != len(d2.Fields) {
		return false
	}
	// 比较 d1 和 d2 的每一个字段
	for i := 0; i < length; i++ {
		f1 := d1.Fields[i]
		f2 := d2.Fields[i]
		// 若 f1 和 f2 的字段名或字段类型或表名不相等，则返回 false
		if f1.Fname != f2.Fname || f1.Ftype != f2.Ftype || f1.TableQualifier != f2.TableQualifier {
			return false
		}
	}
	return true

}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	// 初始化一个FieldType类型的切片
	fields := make([]FieldType, len(td.Fields))
	// 将 td.Fields 的内容复制到 fields 中
	copy(fields, td.Fields)
	return &TupleDesc{Fields: fields} //replace me
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	// 初始化一个FieldType类型的切片，长度为 desc.Fields 的长度加上 desc2.Fields 的长度
	length1 := len(desc.Fields)
	length2 := len(desc2.Fields)
	fields := make([]FieldType, length1, length1+length2)
	// 将 desc.Fields 的内容复制到 fields 中
	copy(fields, desc.Fields)
	// 将 desc2.Fields 的内容添加到 fields 中
	fields = append(fields, desc2.Fields...)
	return &TupleDesc{Fields: fields} //replace me
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	// 遍历 t.Fields，将每个字段的值写入 b 中
	for i, field := range t.Fields {
		// 初始化一个长度为 StringLength 的字节数组
		data := make([]byte, StringLength)
		// 判断数据类型
		if t.Desc.Fields[i].Ftype == IntType {
			// 若为 IntType，则将 int64 转换为 uint64，再将 uint64 转换为字节数组
			binary.LittleEndian.PutUint64(data, uint64(field.(IntField).Value))
		} else if t.Desc.Fields[i].Ftype == StringType {
			// 若为 StringType，则将 string 转换为字节数组
			copy(data, []byte(field.(StringField).Value))
		}
		// 将 data 写入 b 中
		err := binary.Write(b, binary.LittleEndian, data)
		if err != nil {
			return err
		}
	}
	return nil //replace me
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	length := len(desc.Fields)
	// fields 存储每个字段的值
	fields := make([]DBValue, length)
	// 构造一个二维数组，用于存储读取到的byte数据，每个元素的长度为 StringLength
	var data = make([][StringLength]byte, length)
	// 从 b 中读取数据
	err := binary.Read(b, binary.LittleEndian, data)
	if err != nil {
		return nil, err
	}
	// 遍历 desc.Fields，将每个字段的值存储到 fields 中
	for i, v := range desc.Fields {
		// 判断数据类型
		if v.Ftype == IntType {
			// 若为 IntType，则将字节数组转换为 uint64，再将 uint64 转换为 int64
			var field_value IntField
			field_value.Value = int64(binary.LittleEndian.Uint64(data[i][:]))
			if err != nil {
				return nil, err
			}
			fields[i] = field_value
		} else if v.Ftype == StringType {
			// 若为 StringType，则将字节数组转换为 string
			var field_value StringField
			// 去除字符串中的空字符
			c := bytes.Trim(data[i][:], "\x00")
			field_value.Value = string(c)
			fields[i] = field_value
		}
	}
	// 返回一个 Tuple 对象
	return &Tuple{
		Desc:   *desc.copy(),
		Fields: fields,
		Rid:    0,
	}, nil //replace me

}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	// 先判断 t1 和 t2 的 TupleDesc 是否相等
	if !t1.Desc.equals(&t2.Desc) {
		return false
	}
	// 再判断 t1 和 t2 的 Fields 是否相等
	// 若长度不相等，则返回 false
	length := len(t1.Fields)
	if length != len(t2.Fields) {
		return false
	}
	// 遍历 t1 和 t2 的 Fields，比较每个字段的值
	for i := 0; i < length; i++ {
		if t1.Fields[i] != t2.Fields[i] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	// 合并 t1 和 t2 的 TupleDesc
	desc := t1.Desc.merge(&t2.Desc)
	// 合并 t1 和 t2 的 Fields
	fields := append(t1.Fields, t2.Fields...)
	return &Tuple{
		Desc:   *desc,
		Fields: fields,
	}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	// 调用 Expr.EvalExpr 方法，获取 t 和 t2 中字段的值
	val1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}
	val2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}
	// 判断字段的类型
	typeName := field.GetExprType().Ftype
	if typeName == IntType {
		// 若为 IntType，则进行IntField类型的比较
		value1 := val1.(IntField).Value
		value2 := val2.(IntField).Value
		if value1 < value2 {
			return OrderedLessThan, nil
		} else if value1 > value2 {
			return OrderedGreaterThan, nil
		} else {
			return OrderedEqual, nil
		}
	} else if typeName == StringType {
		// 若为 StringType，则进行StringField类型的比较
		value1 := val1.(StringField).Value
		value2 := val2.(StringField).Value
		if value1 < value2 {
			return OrderedLessThan, nil
		} else if value1 > value2 {
			return OrderedGreaterThan, nil
		} else {
			return OrderedEqual, nil
		}
	}
	return OrderedEqual, GoDBError{
		code:      0,
		errString: "err",
	} // replace me
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	// 初始化一个 DBValue 类型的切片
	values := make([]DBValue, len(fields))
	// 依次获取 fields 中对应的字段的值
	for i, v := range fields {
		ans := -1
		// 遍历 t.Desc.Fields，找到对应的字段
		for j, val := range t.Desc.Fields {
			if val.Fname == v.Fname {
				ans = j
				// 只有当 TableQualifier 相等时，为精准匹配
				if val.TableQualifier == v.TableQualifier {
					break
				}
			}
		}
		// 若未找到对应的字段，则返回错误
		if ans == -1 {
			return nil, GoDBError{}
		}
		// 将字段的值存储到 values 中
		values[i] = t.Fields[ans]
	}

	return &Tuple{
		Fields: values,
	}, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
