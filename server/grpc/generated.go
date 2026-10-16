package grpc

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Direct conversions read and write the Go struct of a generated message with Go reflection, through the
// fields its protobuf struct tags name, rather than through protoreflect, which looks every field up on
// each access and boxes the values it sets. They follow protoreflect's rules for generated structs: when
// a field is set, how an empty bytes field keeps its presence, and how a oneof holds its member. Messages
// of the opaque API, whose fields hide behind presence bits and lazy decoding, and messages of any other
// implementation, such as dynamicpb, keep the protoreflect conversions. TestDirectBinding and
// FuzzDirectBinding compare the two.

var (
	timestampPtrType = reflect.TypeFor[*timestamppb.Timestamp]()
	durationPtrType  = reflect.TypeFor[*durationpb.Duration]()
	structPtrType    = reflect.TypeFor[*structpb.Struct]()
	valuePtrType     = reflect.TypeFor[*structpb.Value]()
	listValuePtrType = reflect.TypeFor[*structpb.ListValue]()
	fieldMaskPtrType = reflect.TypeFor[*fieldmaskpb.FieldMask]()

	// emptyBytes and nilBytes are what protoreflect stores for empty bytes: a field with explicit presence
	// keeps it with an empty, non-nil slice, and any other drops it.
	emptyBytes = reflect.ValueOf([]byte{})
	nilBytes   = reflect.Zero(reflect.TypeFor[[]byte]())
)

// genLayout is the Go struct of a generated message, and where it holds each field of the message.
type genLayout struct {
	typ    reflect.Type // the struct
	ptr    reflect.Type // the message's Go type, a pointer to the struct
	fields map[protoreflect.FieldNumber]*genAccess
}

// layoutOf returns the layout of the Go struct of mt's messages, or nil when mt's messages are not generated
// structs of the open or hybrid API, so that only protoreflect reads and writes their fields correctly.
func layoutOf(mt protoreflect.MessageType) *genLayout {
	m := mt.New()
	ptr := reflect.TypeOf(m.Interface())
	if ptr.Kind() != reflect.Pointer || ptr.Elem().Kind() != reflect.Struct {
		return nil
	}
	st := ptr.Elem()
	numbers := map[protoreflect.FieldNumber]int{}
	oneofs := map[string]int{}
	for i := range st.NumField() {
		sf := st.Field(i)
		switch {
		case strings.HasPrefix(sf.Tag.Get("protogen"), "opaque."), sf.Name == "XXX_presence", sf.Name == "XXX_lazyUnmarshalInfo":
			return nil
		}
		if tag, ok := sf.Tag.Lookup("protobuf"); ok {
			n, ok := tagNumber(tag)
			if !ok || !sf.IsExported() {
				return nil
			}
			numbers[n] = i
		}
		if name, ok := sf.Tag.Lookup("protobuf_oneof"); ok {
			if !sf.IsExported() || sf.Type.Kind() != reflect.Interface {
				return nil
			}
			oneofs[name] = i
		}
	}

	layout := &genLayout{typ: st, ptr: ptr, fields: map[protoreflect.FieldNumber]*genAccess{}}
	fds := mt.Descriptor().Fields()
	for i := range fds.Len() {
		fd := fds.Get(i)
		var a *genAccess
		if od := fd.ContainingOneof(); od != nil && !od.IsSynthetic() {
			index, ok := oneofs[string(od.Name())]
			if !ok {
				return nil
			}
			a = oneofAccess(m, fd, index)
		} else {
			index, ok := numbers[fd.Number()]
			if !ok {
				return nil
			}
			a = fieldAccess(fd, index, st.Field(index).Type)
		}
		if a == nil || !genTypeFits(fd, a.typ) {
			return nil
		}
		layout.fields[fd.Number()] = a
	}
	return layout
}

// tagNumber returns the field number of a protobuf struct tag, such as "bytes,1,opt,name=id,proto3".
func tagNumber(tag string) (protoreflect.FieldNumber, bool) {
	parts := strings.SplitN(tag, ",", 3)
	if len(parts) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(parts[1])
	return protoreflect.FieldNumber(n), err == nil
}

// genTypeFits reports whether t, the Go type of a field's value in a generated struct, is the type
// protoreflect gives fd, so that the direct conversions never meet another.
func genTypeFits(fd protoreflect.FieldDescriptor, t reflect.Type) bool {
	switch {
	case fd.IsMap():
		return t.Kind() == reflect.Map && genValueFits(fd.MapKey(), t.Key()) && genValueFits(fd.MapValue(), t.Elem())
	case fd.IsList():
		return t.Kind() == reflect.Slice && genValueFits(fd, t.Elem())
	default:
		return genValueFits(fd, t)
	}
}

func genValueFits(fd protoreflect.FieldDescriptor, t reflect.Type) bool {
	if fd.Message() != nil {
		return t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct && t.Implements(protoMessageType)
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return t.Kind() == reflect.Bool
	case protoreflect.EnumKind, protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return t.Kind() == reflect.Int32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return t.Kind() == reflect.Int64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return t.Kind() == reflect.Uint32
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return t.Kind() == reflect.Uint64
	case protoreflect.FloatKind:
		return t.Kind() == reflect.Float32
	case protoreflect.DoubleKind:
		return t.Kind() == reflect.Float64
	case protoreflect.StringKind:
		return t.Kind() == reflect.String
	case protoreflect.BytesKind:
		return t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8
	}
	return false
}

// accessKind is how a generated struct holds a field.
type accessKind uint8

const (
	accessImplicit accessKind = iota // a scalar, set when it is not its zero value
	accessPointer                    // a scalar with explicit presence: a pointer, nil when not set
	accessBytes                      // bytes with explicit presence: nil when not set
	accessMessage                    // a message: a pointer, nil when not set
	accessRepeated                   // a list or a map: set when not empty
	accessOneof                      // a member of a oneof: a wrapper in the oneof's interface field
)

// genAccess reads and writes a field of a generated struct.
type genAccess struct {
	kind  accessKind
	index int          // of the struct field, or of the oneof's interface field
	typ   reflect.Type // of the field's value: the field's type, its element for a pointer, the wrapper's field
	// wrapper is the pointer to the wrapper struct of a oneof member.
	wrapper reflect.Type
	// bytes is set for a singular bytes field outside a oneof, whose empty value says whether it is set.
	bytes bool
}

func fieldAccess(fd protoreflect.FieldDescriptor, index int, t reflect.Type) *genAccess {
	a := &genAccess{index: index, typ: t}
	switch {
	case fd.IsList() || fd.IsMap():
		a.kind = accessRepeated
	case fd.Message() != nil:
		a.kind = accessMessage
	case fd.HasPresence() && t.Kind() == reflect.Pointer:
		a.kind, a.typ = accessPointer, t.Elem()
	case fd.HasPresence() && fd.Kind() == protoreflect.BytesKind:
		a.kind, a.bytes = accessBytes, true
	case fd.HasPresence():
		return nil // generated code never has it
	default:
		a.kind, a.bytes = accessImplicit, fd.Kind() == protoreflect.BytesKind
	}
	return a
}

// oneofAccess returns the access to fd, a member of a oneof held in the interface field at index. It sets
// fd in m, an empty message, to learn the member's wrapper type, then clears it.
func oneofAccess(m protoreflect.Message, fd protoreflect.FieldDescriptor, index int) *genAccess {
	m.Set(fd, m.NewField(fd))
	w := reflect.ValueOf(m.Interface()).Elem().Field(index).Elem()
	m.Clear(fd)
	if w.Kind() != reflect.Pointer || w.Elem().Kind() != reflect.Struct || w.Elem().NumField() != 1 {
		return nil
	}
	return &genAccess{kind: accessOneof, index: index, typ: w.Type().Elem().Field(0).Type, wrapper: w.Type()}
}

// get returns the value of the field in gen, and whether it is set.
func (a *genAccess) get(gen reflect.Value) (reflect.Value, bool) {
	f := gen.Field(a.index)
	switch a.kind {
	case accessPointer:
		if f.IsNil() {
			return f, false
		}
		return f.Elem(), true
	case accessBytes, accessMessage:
		return f, !f.IsNil()
	case accessOneof:
		if f.IsNil() || f.Elem().Type() != a.wrapper || f.Elem().IsNil() {
			return f, false
		}
		return f.Elem().Elem().Field(0), true
	}
	switch f.Kind() {
	case reflect.Bool:
		return f, f.Bool()
	case reflect.Int32, reflect.Int64:
		return f, f.Int() != 0
	case reflect.Uint32, reflect.Uint64:
		return f, f.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return f, f.Float() != 0 || math.Signbit(f.Float())
	default: // string, bytes, list, map
		return f, f.Len() > 0
	}
}

// set returns where to write the value of the field in gen, which it makes room for: a new pointer for a
// scalar with explicit presence, a new wrapper for a oneof member. Call finish once the value is written.
func (a *genAccess) set(gen reflect.Value) reflect.Value {
	f := gen.Field(a.index)
	switch a.kind {
	case accessPointer:
		p := reflect.New(a.typ)
		f.Set(p)
		return p.Elem()
	case accessOneof:
		w := reflect.New(a.wrapper.Elem())
		f.Set(w)
		return w.Elem().Field(0)
	}
	return f
}

// finish stores an empty bytes value the way protoreflect does.
func (a *genAccess) finish(gen reflect.Value) {
	if !a.bytes {
		return
	}
	if f := gen.Field(a.index); f.Len() == 0 {
		if a.kind == accessBytes {
			f.Set(emptyBytes)
		} else {
			f.Set(nilBytes)
		}
	}
}

// genConv converts a value in a generated struct, of a field, a list element or a map entry, and a Go value.
type genConv struct {
	// toGo sets dst, which is addressable and holds its zero value, from src, a generated value.
	toGo func(src, dst reflect.Value) error
	// toGen sets dst, a settable generated value holding its zero value, from src.
	toGen func(src, dst reflect.Value) error
}

// genValue returns the conversion of a generated value of fd, of type gt, and a Go value of type t.
func (p *planner) genValue(fd protoreflect.FieldDescriptor, gt, t reflect.Type) (genConv, error) {
	if md := fd.Message(); md != nil {
		return p.genMessageValue(md, gt, t)
	}
	return p.genScalar(fd, t)
}

// genElement returns the conversion of a list element or a map value, which may be a pointer to the struct
// of a message, like element.
func (p *planner) genElement(fd protoreflect.FieldDescriptor, gt, t reflect.Type) (genConv, error) {
	if t.Kind() != reflect.Pointer || isProtoPointer(t) {
		return p.genValue(fd, gt, t)
	}
	if fd.Message() == nil {
		return genConv{}, fmt.Errorf("cannot map %s to %s", fd.Kind(), t)
	}
	conv, err := p.genValue(fd, gt, t.Elem())
	if err != nil {
		return genConv{}, err
	}
	elem := t.Elem()
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			ptr := reflect.New(elem)
			if err := conv.toGo(src, ptr.Elem()); err != nil {
				return err
			}
			dst.Set(ptr)
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			if src.IsNil() {
				return errNilElement
			}
			return conv.toGen(src.Elem(), dst)
		},
	}, nil
}

// genScalar converts a scalar or an enum like scalar does, reading and writing the generated value
// directly. The protoreflect.Value between the two holds a scalar inline, without allocating. A string,
// bytes or bool to a Go value of the same kind is copied as it is.
func (p *planner) genScalar(fd protoreflect.FieldDescriptor, t reflect.Type) (genConv, error) {
	conv, err := p.scalar(fd, t)
	if err != nil {
		return genConv{}, err
	}
	if _, isText := p.text(t); !isText {
		switch {
		case fd.Kind() == protoreflect.StringKind && t.Kind() == reflect.String:
			return genConv{toGo: copyString, toGen: copyString}, nil
		case fd.Kind() == protoreflect.BytesKind:
			return genConv{toGo: copyBytes, toGen: copyBytes}, nil
		case fd.Kind() == protoreflect.BoolKind:
			return genConv{toGo: copyBool, toGen: copyBool}, nil
		}
	}
	get, set := scalarGet(fd.Kind()), scalarSet(fd.Kind())
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			return conv.toGo(get(src), dst)
		},
		toGen: func(src, dst reflect.Value) error {
			v, err := conv.toProto(src)
			if err != nil {
				return err
			}
			set(dst, v)
			return nil
		},
	}, nil
}

func copyString(src, dst reflect.Value) error {
	dst.SetString(src.String())
	return nil
}

func copyBytes(src, dst reflect.Value) error {
	dst.SetBytes(src.Bytes())
	return nil
}

func copyBool(src, dst reflect.Value) error {
	dst.SetBool(src.Bool())
	return nil
}

// scalarGet returns the function that reads a generated value of kind.
func scalarGet(kind protoreflect.Kind) func(reflect.Value) protoreflect.Value {
	switch kind {
	case protoreflect.BoolKind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfBool(v.Bool()) }
	case protoreflect.EnumKind:
		return func(v reflect.Value) protoreflect.Value {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v.Int()))
		}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfInt32(int32(v.Int())) }
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfInt64(v.Int()) }
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfUint32(uint32(v.Uint())) }
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfUint64(v.Uint()) }
	case protoreflect.FloatKind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfFloat32(float32(v.Float())) }
	case protoreflect.DoubleKind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfFloat64(v.Float()) }
	case protoreflect.StringKind:
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfString(v.String()) }
	default: // protoreflect.BytesKind
		return func(v reflect.Value) protoreflect.Value { return protoreflect.ValueOfBytes(v.Bytes()) }
	}
}

// scalarSet returns the function that writes a generated value of kind.
func scalarSet(kind protoreflect.Kind) func(reflect.Value, protoreflect.Value) {
	switch kind {
	case protoreflect.BoolKind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetBool(v.Bool()) }
	case protoreflect.EnumKind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetInt(int64(v.Enum())) }
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetInt(v.Int()) }
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind, protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetUint(v.Uint()) }
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetFloat(v.Float()) }
	case protoreflect.StringKind:
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetString(v.String()) }
	default: // protoreflect.BytesKind
		return func(dst reflect.Value, v protoreflect.Value) { dst.SetBytes(v.Bytes()) }
	}
}

// genMessageValue converts a generated message, of type gt, and a Go value, like messageValue.
func (p *planner) genMessageValue(md protoreflect.MessageDescriptor, gt, t reflect.Type) (genConv, error) {
	name := md.FullName()
	switch {
	case isProtoPointer(t) && t == gt:
		if _, err := protoValue(md, t); err != nil {
			return genConv{}, err
		}
		return genConv{
			toGo: func(src, dst reflect.Value) error {
				dst.Set(src)
				return nil
			},
			toGen: func(src, dst reflect.Value) error {
				if src.IsNil() {
					return errNilElement
				}
				dst.Set(src)
				return nil
			},
		}, nil
	case name == "google.protobuf.Timestamp" && t == timeType && gt == timestampPtrType:
		return genTimestamp(), nil
	case name == "google.protobuf.Duration" && t == durationType && gt == durationPtrType:
		return genDuration(), nil
	case wrapperMessages[name] && t.Kind() != reflect.Struct:
		if conv, ok, err := p.genWrapper(md, gt, t); ok || err != nil {
			return conv, err
		}
	case name == "google.protobuf.Struct" && gt == structPtrType && t.Kind() == reflect.Map &&
		t.Key().Kind() == reflect.String && t.Elem() == anyType:
		return genStruct(t), nil
	case name == "google.protobuf.Value" && gt == valuePtrType && t == anyType:
		return genDynamic(), nil
	case name == "google.protobuf.ListValue" && gt == listValuePtrType && t.Kind() == reflect.Slice && t.Elem() == anyType:
		return genList(t), nil
	case name == "google.protobuf.FieldMask" && gt == fieldMaskPtrType && t.Kind() == reflect.Slice &&
		t.Elem().Kind() == reflect.String:
		return genFieldMask(t), nil
	case t.Kind() == reflect.Struct:
		plan, err := p.message(md, t)
		if err != nil {
			return genConv{}, err
		}
		if plan.layout == nil || plan.layout.ptr != gt {
			break
		}
		return genConv{
			toGo: func(src, dst reflect.Value) error {
				if src.IsNil() {
					return nil // an empty message, as protoreflect reads it
				}
				return plan.fromGen(src.Elem(), dst)
			},
			toGen: func(src, dst reflect.Value) error {
				ptr := reflect.New(plan.layout.typ)
				if err := plan.toGen(src, ptr.Elem()); err != nil {
					return err
				}
				dst.Set(ptr)
				return nil
			},
		}, nil
	}
	conv, err := p.messageValue(md, t)
	if err != nil {
		return genConv{}, err
	}
	return bridge(conv), nil
}

// bridge converts a generated message with conv, the protoreflect conversion, for the messages the direct
// conversions leave to it, such as a message of the opaque API in a field of an open one.
func bridge(conv valueConv) genConv {
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			return conv.toGo(protoreflect.ValueOfMessage(src.Interface().(proto.Message).ProtoReflect()), dst)
		},
		toGen: func(src, dst reflect.Value) error {
			v, err := conv.toProto(src)
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(v.Message().Interface()))
			return nil
		},
	}
}

// genTimestamp converts a *timestamppb.Timestamp and a time.Time, like timestampValue.
func genTimestamp() genConv {
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			ts := src.Interface().(*timestamppb.Timestamp)
			t, err := timeOf(ts.GetSeconds(), int64(ts.GetNanos()))
			if err != nil {
				return err
			}
			setAs(dst, t)
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			s, n, err := timestampOf(valueAs[time.Time](src))
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(&timestamppb.Timestamp{Seconds: s, Nanos: n}))
			return nil
		},
	}
}

// genDuration converts a *durationpb.Duration and a time.Duration, like durationValue.
func genDuration() genConv {
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			d := src.Interface().(*durationpb.Duration)
			v, err := durationOf(d.GetSeconds(), int64(d.GetNanos()))
			if err != nil {
				return err
			}
			dst.SetInt(int64(v))
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			d := time.Duration(src.Int())
			dst.Set(reflect.ValueOf(&durationpb.Duration{Seconds: int64(d / time.Second), Nanos: int32(d % time.Second)}))
			return nil
		},
	}
}

// genWrapper converts a wrapper message, such as *wrapperspb.StringValue, like its value field. It reports
// false when gt has no layout, for the protoreflect conversion.
func (p *planner) genWrapper(md protoreflect.MessageDescriptor, gt, t reflect.Type) (genConv, bool, error) {
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		return genConv{}, false, nil
	}
	layout := layoutOf(mt)
	if layout == nil || layout.ptr != gt {
		return genConv{}, false, nil
	}
	fd := md.Fields().ByName("value")
	inner, err := p.genScalar(fd, t)
	if err != nil {
		return genConv{}, false, err
	}
	index := layout.fields[fd.Number()].index
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			if src.IsNil() {
				return nil // its value is the zero value, which dst holds
			}
			return inner.toGo(src.Elem().Field(index), dst)
		},
		toGen: func(src, dst reflect.Value) error {
			ptr := reflect.New(gt.Elem())
			if err := inner.toGen(src, ptr.Elem().Field(index)); err != nil {
				return err
			}
			dst.Set(ptr)
			return nil
		},
	}, true, nil
}

// genStruct converts a *structpb.Struct and a map of string to any, like structValue.
func genStruct(t reflect.Type) genConv {
	mapType := reflect.TypeFor[map[string]any]()
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			dst.Set(reflect.ValueOf(src.Interface().(*structpb.Struct).AsMap()).Convert(t))
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			s, err := structpb.NewStruct(src.Convert(mapType).Interface().(map[string]any))
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(s))
			return nil
		},
	}
}

// genDynamic converts a *structpb.Value and an any, like dynamicValue.
func genDynamic() genConv {
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			if x := src.Interface().(*structpb.Value).AsInterface(); x != nil {
				dst.Set(reflect.ValueOf(x))
			}
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			v, err := structpb.NewValue(src.Interface())
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(v))
			return nil
		},
	}
}

// genList converts a *structpb.ListValue and a slice of any, like listValue.
func genList(t reflect.Type) genConv {
	sliceType := reflect.TypeFor[[]any]()
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			dst.Set(reflect.ValueOf(src.Interface().(*structpb.ListValue).AsSlice()).Convert(t))
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			l, err := structpb.NewList(src.Convert(sliceType).Interface().([]any))
			if err != nil {
				return err
			}
			dst.Set(reflect.ValueOf(l))
			return nil
		},
	}
}

// genFieldMask converts a *fieldmaskpb.FieldMask and a slice of its paths, like fieldMaskValue.
func genFieldMask(t reflect.Type) genConv {
	return genConv{
		toGo: func(src, dst reflect.Value) error {
			paths := src.Interface().(*fieldmaskpb.FieldMask).GetPaths()
			s := reflect.MakeSlice(t, len(paths), len(paths))
			for i, path := range paths {
				s.Index(i).SetString(path)
			}
			dst.Set(s)
			return nil
		},
		toGen: func(src, dst reflect.Value) error {
			paths := make([]string, src.Len())
			for i := range paths {
				paths[i] = src.Index(i).String()
			}
			dst.Set(reflect.ValueOf(&fieldmaskpb.FieldMask{Paths: paths}))
			return nil
		},
	}
}

// valueAs returns v, of type T, without the allocation of v.Interface() when v is addressable.
func valueAs[T any](v reflect.Value) T {
	if v.CanAddr() {
		return *v.Addr().Interface().(*T)
	}
	return v.Interface().(T)
}

// setAs sets dst, of type T, to x, without the allocation of reflect.ValueOf(x) when dst is addressable.
func setAs[T any](dst reflect.Value, x T) {
	if dst.CanAddr() {
		*dst.Addr().Interface().(*T) = x
		return
	}
	dst.Set(reflect.ValueOf(x))
}
