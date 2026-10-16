package grpc

import (
	"context"
	"encoding"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	protoMessageType    = reflect.TypeFor[proto.Message]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	timeType            = reflect.TypeFor[time.Time]()
	durationType        = reflect.TypeFor[time.Duration]()
	anyType             = reflect.TypeFor[any]()

	errNilElement = errors.New("nil element")
)

// The wrapper messages, which convert like the scalar of their value field.
var wrapperMessages = map[protoreflect.FullName]bool{
	"google.protobuf.DoubleValue": true,
	"google.protobuf.FloatValue":  true,
	"google.protobuf.Int64Value":  true,
	"google.protobuf.UInt64Value": true,
	"google.protobuf.Int32Value":  true,
	"google.protobuf.UInt32Value": true,
	"google.protobuf.BoolValue":   true,
	"google.protobuf.StringValue": true,
	"google.protobuf.BytesValue":  true,
}

// The range of valid google.protobuf.Timestamp seconds: 0001-01-01 to 9999-12-31.
const (
	minTimestampSeconds = -62135596800
	maxTimestampSeconds = 253402300799
)

// binding converts between a proto message of a method and T, the request or response type of its handler.
// New builds it, so that a type that does not fit the message fails at startup. T is either a pointer to the
// proto message itself, which is passed as it is, or a struct, or a pointer to one, whose fields are
// converted from and to the message's fields.
type binding[T any] struct {
	// name names T in validation errors: the proto message, or the Go type.
	name string
	// newMessage returns an empty message of the method's type.
	newMessage func() proto.Message
	// decode converts a received message, and also returns a pointer to the result, for the validator: the
	// value itself when T is a pointer. ctx holds the call's metadata, for the header fields of a struct. It
	// is set for requests only.
	decode func(ctx context.Context, m proto.Message) (T, any, error)
	// validate validates the pointer decode returns. It is nil when nothing validates T. New sets it.
	validate func(any) error
	// toProto converts a value to send. It is set for responses only.
	toProto func(v T) (proto.Message, error)
	// direct reports that the binding converts the method's messages directly, as generated structs.
	direct bool
}

// newBinding returns the binding of T and the message md, which is nil when the service's descriptor is not
// in the proto registry. decode builds toGo, for a request, and otherwise toProto, for a response. The
// binding converts generated messages directly, and others through protoreflect.
func newBinding[T any](md protoreflect.MessageDescriptor, decode bool) (binding[T], error) {
	return newBindingVia[T](md, decode, true)
}

// newBindingVia returns the binding of newBinding, which converts every message through protoreflect when
// direct is false, for the tests that compare the two.
func newBindingVia[T any](md protoreflect.MessageDescriptor, decode, direct bool) (binding[T], error) {
	t := reflect.TypeFor[T]()
	if t.Implements(protoMessageType) {
		return protoBinding[T](t, md)
	}
	if reflect.PointerTo(t).Implements(protoMessageType) {
		return binding[T]{}, fmt.Errorf("%s is a proto message, which is used through a pointer: *%[1]s", t)
	}
	st := t
	if st.Kind() == reflect.Pointer {
		st = st.Elem()
	}
	if st.Kind() != reflect.Struct {
		return binding[T]{}, fmt.Errorf("%s is neither a pointer to a proto message nor a struct", t)
	}
	if md == nil {
		return binding[T]{}, fmt.Errorf("%s is not a proto message, and the service's descriptor is not in the proto registry", t)
	}

	p := planner{decode: decode, direct: direct, plans: map[planKey]*messagePlan{}}
	plan, err := p.message(md, st)
	if err != nil {
		return binding[T]{}, err
	}
	pointer := t.Kind() == reflect.Pointer
	b := binding[T]{
		name:       t.String(),
		newMessage: func() proto.Message { return plan.mt.New().Interface() },
		direct:     plan.layout != nil,
	}
	if !decode {
		b.toProto = func(v T) (proto.Message, error) {
			src := reflect.ValueOf(&v).Elem()
			if pointer {
				if src.IsNil() {
					return nil, errNilResponse
				}
				src = src.Elem()
			}
			if plan.layout != nil {
				ptr := reflect.New(plan.layout.typ)
				if err := plan.toGen(src, ptr.Elem()); err != nil {
					return nil, errInvalidResponse.Wrap(err)
				}
				return ptr.Interface().(proto.Message), nil
			}
			m, err := plan.toProto(src)
			if err != nil {
				return nil, errInvalidResponse.Wrap(err)
			}
			return m.Interface(), nil
		}
		return b, nil
	}

	fields, _, err := structFields(st)
	if err != nil {
		return binding[T]{}, err
	}
	headers, err := headerPlans(st, fields)
	if err != nil {
		return binding[T]{}, err
	}
	b.decode = func(ctx context.Context, msg proto.Message) (T, any, error) {
		var zero T
		gen := reflect.ValueOf(msg)
		direct := plan.layout != nil && gen.Type() == plan.layout.ptr
		if !direct {
			if name := msg.ProtoReflect().Descriptor().FullName(); name != plan.name {
				return zero, nil, fmt.Errorf("grpc: the request is %s, not %s", name, plan.name)
			}
		}
		// The value is filled on the heap, where the validator gets it, and copied to the stack only once.
		var ptr reflect.Value
		if pointer {
			ptr = reflect.New(st)
		} else {
			ptr = reflect.ValueOf(new(T))
		}
		dst := ptr.Elem()
		var err error
		if direct {
			err = plan.fromGen(gen.Elem(), dst)
		} else {
			err = plan.toGo(msg.ProtoReflect(), dst)
		}
		if err != nil {
			return zero, nil, err
		}
		for i := range headers {
			h := &headers[i]
			values := metadata.ValueFromIncomingContext(ctx, h.key)
			if len(values) == 0 {
				continue
			}
			if err := h.set(values, dst.FieldByIndex(h.index)); err != nil {
				return zero, nil, &fieldError{source: sourceMetadata, path: []string{h.key}, cause: err}
			}
		}
		target := ptr.Interface()
		if pointer {
			return target.(T), target, nil
		}
		return *target.(*T), target, nil
	}
	return b, nil
}

// toGo converts a received message, as decode does.
func (b binding[T]) toGo(ctx context.Context, m proto.Message) (T, error) {
	v, _, err := b.decode(ctx, m)
	return v, err
}

// protoBinding returns the binding of T, a proto message passed as it is, which must be md's message when md
// is known.
func protoBinding[T any](t reflect.Type, md protoreflect.MessageDescriptor) (binding[T], error) {
	if t.Kind() != reflect.Pointer {
		return binding[T]{}, fmt.Errorf("%s is not a pointer to a proto message", t)
	}
	m := reflect.New(t.Elem()).Interface().(proto.Message).ProtoReflect()
	name := m.Descriptor().FullName()
	if md != nil && name != md.FullName() {
		return binding[T]{}, fmt.Errorf("%s is %s, but the method's is %s", t, name, md.FullName())
	}
	elem := t.Elem()
	return binding[T]{
		name:       string(name),
		newMessage: func() proto.Message { return reflect.New(elem).Interface().(proto.Message) },
		decode: func(_ context.Context, msg proto.Message) (T, any, error) {
			v, ok := msg.(T)
			if !ok {
				return v, nil, fmt.Errorf("grpc: the request is %T, not %s", msg, t)
			}
			return v, msg, nil
		},
		toProto: func(v T) (proto.Message, error) {
			msg := any(v).(proto.Message)
			if !msg.ProtoReflect().IsValid() {
				return nil, errNilResponse
			}
			return msg, nil
		},
	}, nil
}

// fieldError is a value of a request that its handler's type cannot hold, such as a number out of its
// range, or a string its UnmarshalText rejects. MapBinding maps it.
type fieldError struct {
	// source is where the value came from: a field of the message, or the call's metadata.
	source string
	// path holds the names of the field and of the fields it is nested in, innermost first.
	path  []string
	cause error
}

const (
	sourceField    = "field"
	sourceMetadata = "metadata"
)

// Path returns the dotted path of the field, such as "address.zip".
func (e *fieldError) Path() string {
	var b strings.Builder
	for i, name := range slices.Backward(e.path) {
		if i < len(e.path)-1 {
			b.WriteByte('.')
		}
		b.WriteString(name)
	}
	return b.String()
}

func (e *fieldError) Error() string {
	return e.source + " " + e.Path() + ": " + e.cause.Error()
}

func (e *fieldError) Unwrap() error {
	return e.cause
}

// atField returns err, the failure to convert the field name, with name prepended to its path.
func atField(err error, name string) error {
	var fe *fieldError
	if errors.As(err, &fe) {
		fe.path = append(fe.path, name)
		return fe
	}
	return &fieldError{source: sourceField, path: []string{name}, cause: err}
}

// planner builds the plans that convert messages and structs, in one direction. It keeps every plan it
// built, so that recursive messages end.
type planner struct {
	decode bool
	// direct plans direct conversions for the messages that have a layout.
	direct bool
	plans  map[planKey]*messagePlan
}

type planKey struct {
	message protoreflect.FullName
	t       reflect.Type
}

// messagePlan converts a message and a struct type, field by field.
type messagePlan struct {
	name protoreflect.FullName
	mt   protoreflect.MessageType
	// layout is the Go struct of the generated messages of mt, which the plan converts directly. It is nil
	// for the messages that only convert through protoreflect.
	layout *genLayout
	fields []fieldPlan
	// oneofs holds, for each oneof with more than one field, the indexes of its fields in fields. At most one
	// of them may be set.
	oneofs [][]int
}

// fieldPlan converts a field of a message and the field of the struct at index.
type fieldPlan struct {
	name    string // the proto field's name, in error paths
	index   []int
	toGo    func(m protoreflect.Message, dst reflect.Value) error
	toProto func(src reflect.Value, m protoreflect.Message) error
	// fromGen and toGen are toGo and toProto for gen, the Go struct of a generated message. They are set when
	// the plan has a layout.
	fromGen func(gen, dst reflect.Value) error
	toGen   func(src, gen reflect.Value) error
}

// toGo sets the fields of dst, an addressable struct, from m.
func (p *messagePlan) toGo(m protoreflect.Message, dst reflect.Value) error {
	for i := range p.fields {
		f := &p.fields[i]
		if err := f.toGo(m, dst.FieldByIndex(f.index)); err != nil {
			return atField(err, f.name)
		}
	}
	return nil
}

// toProto returns a new message with the fields of src, a struct.
func (p *messagePlan) toProto(src reflect.Value) (protoreflect.Message, error) {
	if err := p.checkOneofs(src); err != nil {
		return nil, err
	}
	m := p.mt.New()
	for i := range p.fields {
		f := &p.fields[i]
		if err := f.toProto(src.FieldByIndex(f.index), m); err != nil {
			return nil, atField(err, f.name)
		}
	}
	return m, nil
}

// fromGen sets the fields of dst, an addressable struct, from gen, the Go struct of a generated message.
func (p *messagePlan) fromGen(gen, dst reflect.Value) error {
	for i := range p.fields {
		f := &p.fields[i]
		if err := f.fromGen(gen, structField(dst, f.index)); err != nil {
			return atField(err, f.name)
		}
	}
	return nil
}

// structField returns the field of v at index, without FieldByIndex's loop for a field of v itself.
func structField(v reflect.Value, index []int) reflect.Value {
	if len(index) == 1 {
		return v.Field(index[0])
	}
	return v.FieldByIndex(index)
}

// toGen sets the fields of gen, the empty Go struct of a generated message, from src, a struct.
func (p *messagePlan) toGen(src, gen reflect.Value) error {
	if err := p.checkOneofs(src); err != nil {
		return err
	}
	for i := range p.fields {
		f := &p.fields[i]
		if err := f.toGen(structField(src, f.index), gen); err != nil {
			return atField(err, f.name)
		}
	}
	return nil
}

// checkOneofs fails when src sets more than one member of a oneof.
func (p *messagePlan) checkOneofs(src reflect.Value) error {
	for _, members := range p.oneofs {
		var set []string
		for _, i := range members {
			if !src.FieldByIndex(p.fields[i].index).IsNil() {
				set = append(set, p.fields[i].name)
			}
		}
		if len(set) > 1 {
			return fmt.Errorf("%s are members of one oneof, but more than one is set", strings.Join(set, " and "))
		}
	}
	return nil
}

// goField is an exported field of a struct, and the names it maps by.
type goField struct {
	sf reflect.StructField
	// exact is the name of the grpc tag, which must be a field of the message.
	exact string
	// names are the normalized names of the json, uri and query tags, which the struct also binds over HTTP.
	// Without them, the field maps by its Go name.
	names []string
	// header is the lower-case name of the header tag: a request field that comes from the call's metadata.
	header string
}

// structFields returns the fields of t that map to message fields, and the message fields its blank fields
// name in their grpc tags, which a request leaves out.
func structFields(t reflect.Type) ([]goField, map[protoreflect.Name]bool, error) {
	ignored := map[protoreflect.Name]bool{}
	// VisibleFields leaves blank fields out.
	for i := range t.NumField() {
		if sf := t.Field(i); sf.Name == "_" {
			if name, _, _ := strings.Cut(sf.Tag.Get("grpc"), ","); name != "" && name != "-" {
				ignored[protoreflect.Name(name)] = true
			}
		}
	}
	var fields []goField
	for _, sf := range reflect.VisibleFields(t) {
		grpcTag, hasGRPC := sf.Tag.Lookup("grpc")
		grpcName, _, _ := strings.Cut(grpcTag, ",")
		if sf.Anonymous && (sf.Type.Kind() == reflect.Struct || sf.Type.Kind() == reflect.Pointer) {
			continue // its fields are promoted
		}
		if !sf.IsExported() {
			continue
		}
		if throughPointer(t, sf.Index) {
			return nil, nil, fmt.Errorf("%s.%s is promoted from an embedded pointer, which is not supported", t, sf.Name)
		}
		if hasGRPC {
			if grpcName == "-" {
				continue
			}
			if grpcName != "" {
				fields = append(fields, goField{sf: sf, exact: grpcName})
				continue
			}
		}

		f := goField{sf: sf}
		jsonName, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		for _, tag := range []string{"json", "uri", "query"} {
			name, _, _ := strings.Cut(sf.Tag.Get(tag), ",")
			if name != "" && name != "-" {
				f.names = append(f.names, normalize(name))
			}
		}
		if name, _, _ := strings.Cut(sf.Tag.Get("header"), ","); name != "" && name != "-" {
			f.header = strings.ToLower(name)
		}
		if jsonName == "-" && f.names == nil && f.header == "" {
			continue // hidden from JSON, and bound from nothing else
		}
		fields = append(fields, f)
	}
	return fields, ignored, nil
}

// throughPointer reports whether the field at index of t is promoted from an embedded pointer.
func throughPointer(t reflect.Type, index []int) bool {
	for _, i := range index[:len(index)-1] {
		f := t.Field(i)
		if f.Type.Kind() == reflect.Pointer {
			return true
		}
		t = f.Type
	}
	return false
}

// normalize returns name in lower case without underscores, so that "user_id", "userId" and "UserID" match.
func normalize(name string) string {
	return strings.ReplaceAll(strings.ToLower(name), "_", "")
}

// match returns the index in fields of the field that maps to fd, or -1. Fields from the metadata never map to
// a field of a request.
func (p *planner) match(fields []goField, fd protoreflect.FieldDescriptor) (int, error) {
	found := -1
	key := normalize(string(fd.Name()))
	for j, f := range fields {
		var ok bool
		switch {
		case f.exact != "":
			ok = f.exact == string(fd.Name())
		case p.decode && f.header != "":
			ok = false
		case f.names != nil:
			ok = slices.Contains(f.names, key)
		default:
			ok = normalize(f.sf.Name) == key
		}
		if !ok {
			continue
		}
		if found >= 0 {
			return -1, fmt.Errorf("both %s and %s map to %s", fields[found].sf.Name, f.sf.Name, fd.FullName())
		}
		found = j
	}
	return found, nil
}

// message returns the plan of md and the struct t.
func (p *planner) message(md protoreflect.MessageDescriptor, t reflect.Type) (*messagePlan, error) {
	key := planKey{message: md.FullName(), t: t}
	if plan, ok := p.plans[key]; ok {
		return plan, nil
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(md.FullName())
	if err != nil {
		return nil, fmt.Errorf("message %s is not in the proto registry", md.FullName())
	}
	plan := &messagePlan{name: md.FullName(), mt: mt}
	if p.direct {
		plan.layout = layoutOf(mt)
	}
	p.plans[key] = plan

	fields, ignored, err := structFields(t)
	if err != nil {
		return nil, err
	}
	mfs := md.Fields()
	for _, f := range fields {
		if f.exact != "" && mfs.ByName(protoreflect.Name(f.exact)) == nil {
			return nil, fmt.Errorf("%s.%s: %s has no field %q", t, f.sf.Name, md.FullName(), f.exact)
		}
	}
	matched := make([]bool, len(fields))
	oneofs := map[protoreflect.FullName][]int{}
	for i := range mfs.Len() {
		fd := mfs.Get(i)
		j, err := p.match(fields, fd)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", t, err)
		}
		if j < 0 {
			if p.decode && !ignored[fd.Name()] {
				return nil, fmt.Errorf("%s has no field for %s; add one, or ignore it with a blank field tagged grpc:%q",
					t, fd.FullName(), fd.Name())
			}
			continue
		}
		sf := fields[j].sf
		if matched[j] {
			return nil, fmt.Errorf("%s.%s maps to more than one field of %s", t, sf.Name, md.FullName())
		}
		matched[j] = true
		var access *genAccess
		if plan.layout != nil {
			access = plan.layout.fields[fd.Number()]
		}
		f, err := p.field(fd, sf.Type, access)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %s: %w", t, sf.Name, fd.FullName(), err)
		}
		f.name, f.index = string(fd.Name()), sf.Index
		if od := fd.ContainingOneof(); od != nil && !od.IsSynthetic() {
			if k := sf.Type.Kind(); k != reflect.Pointer && k != reflect.Interface {
				return nil, fmt.Errorf("%s.%s: %s is a member of oneof %s, so it needs a pointer, nil when another member is set",
					t, sf.Name, fd.FullName(), od.Name())
			}
			oneofs[od.FullName()] = append(oneofs[od.FullName()], len(plan.fields))
		}
		plan.fields = append(plan.fields, f)
	}

	if !p.decode {
		for j, f := range fields {
			if !matched[j] {
				return nil, fmt.Errorf("%s.%s has no field in %s; tag it grpc:\"-\" to leave it out", t, f.sf.Name, md.FullName())
			}
		}
	}
	for i := range md.Oneofs().Len() {
		if members := oneofs[md.Oneofs().Get(i).FullName()]; len(members) > 1 {
			plan.oneofs = append(plan.oneofs, members)
		}
	}
	return plan, nil
}

// field returns the plan of the field fd and a struct field of type t. Its name and index are left to the
// caller. With access, where a generated struct holds fd, it also plans the direct conversions.
func (p *planner) field(fd protoreflect.FieldDescriptor, t reflect.Type, access *genAccess) (fieldPlan, error) {
	switch {
	case fd.IsMap():
		return p.mapField(fd, t, access)
	case fd.IsList():
		return p.listField(fd, t, access)
	default:
		return p.singularField(fd, t, access)
	}
}

// singularField converts a field that is neither repeated nor a map. A pointer is nil when the field is not
// set. Any other type keeps its zero value when the field is not set, and a zero value is not sent.
func (p *planner) singularField(fd protoreflect.FieldDescriptor, t reflect.Type, access *genAccess) (fieldPlan, error) {
	if t.Kind() == reflect.Pointer && !isProtoPointer(t) {
		conv, err := p.value(fd, t.Elem())
		if err != nil {
			return fieldPlan{}, err
		}
		elem := t.Elem()
		plan := fieldPlan{
			toGo: func(m protoreflect.Message, dst reflect.Value) error {
				if !m.Has(fd) {
					return nil
				}
				v := reflect.New(elem)
				if err := conv.toGo(m.Get(fd), v.Elem()); err != nil {
					return err
				}
				dst.Set(v)
				return nil
			},
			toProto: func(src reflect.Value, m protoreflect.Message) error {
				if src.IsNil() {
					return nil
				}
				v, err := conv.toProto(src.Elem())
				if err != nil {
					return err
				}
				m.Set(fd, v)
				return nil
			},
		}
		if access == nil {
			return plan, nil
		}
		gconv, err := p.genValue(fd, access.typ, elem)
		if err != nil {
			return fieldPlan{}, err
		}
		plan.fromGen = func(gen, dst reflect.Value) error {
			src, ok := access.get(gen)
			if !ok {
				return nil
			}
			v := reflect.New(elem)
			if err := gconv.toGo(src, v.Elem()); err != nil {
				return err
			}
			dst.Set(v)
			return nil
		}
		plan.toGen = func(src, gen reflect.Value) error {
			if src.IsNil() {
				return nil
			}
			if err := gconv.toGen(src.Elem(), access.set(gen)); err != nil {
				return err
			}
			access.finish(gen)
			return nil
		}
		return plan, nil
	}

	conv, err := p.value(fd, t)
	if err != nil {
		return fieldPlan{}, err
	}
	plan := fieldPlan{
		toGo: func(m protoreflect.Message, dst reflect.Value) error {
			if !m.Has(fd) {
				return nil
			}
			return conv.toGo(m.Get(fd), dst)
		},
		toProto: func(src reflect.Value, m protoreflect.Message) error {
			if src.IsZero() {
				return nil
			}
			v, err := conv.toProto(src)
			if err != nil {
				return err
			}
			m.Set(fd, v)
			return nil
		},
	}
	if access == nil {
		return plan, nil
	}
	gconv, err := p.genValue(fd, access.typ, t)
	if err != nil {
		return fieldPlan{}, err
	}
	plan.fromGen = func(gen, dst reflect.Value) error {
		src, ok := access.get(gen)
		if !ok {
			return nil
		}
		return gconv.toGo(src, dst)
	}
	plan.toGen = func(src, gen reflect.Value) error {
		if src.IsZero() {
			return nil
		}
		if err := gconv.toGen(src, access.set(gen)); err != nil {
			return err
		}
		access.finish(gen)
		return nil
	}
	return plan, nil
}

// listField converts a repeated field and a slice.
func (p *planner) listField(fd protoreflect.FieldDescriptor, t reflect.Type, access *genAccess) (fieldPlan, error) {
	if t.Kind() != reflect.Slice {
		return fieldPlan{}, fmt.Errorf("a repeated field needs a slice, not %s", t)
	}
	conv, err := p.element(fd, t.Elem())
	if err != nil {
		return fieldPlan{}, err
	}
	plan := fieldPlan{
		toGo: func(m protoreflect.Message, dst reflect.Value) error {
			if !m.Has(fd) {
				return nil
			}
			l := m.Get(fd).List()
			n := l.Len()
			s := reflect.MakeSlice(t, n, n)
			for i := range n {
				if err := conv.toGo(l.Get(i), s.Index(i)); err != nil {
					return err
				}
			}
			dst.Set(s)
			return nil
		},
		toProto: func(src reflect.Value, m protoreflect.Message) error {
			n := src.Len()
			if n == 0 {
				return nil
			}
			l := m.Mutable(fd).List()
			for i := range n {
				v, err := conv.toProto(src.Index(i))
				if err != nil {
					return err
				}
				l.Append(v)
			}
			return nil
		},
	}
	if access == nil {
		return plan, nil
	}
	gt := access.typ
	gconv, err := p.genElement(fd, gt.Elem(), t.Elem())
	if err != nil {
		return fieldPlan{}, err
	}
	plan.fromGen = func(gen, dst reflect.Value) error {
		l := gen.Field(access.index)
		n := l.Len()
		if n == 0 {
			return nil
		}
		s := reflect.MakeSlice(t, n, n)
		for i := range n {
			if err := gconv.toGo(l.Index(i), s.Index(i)); err != nil {
				return err
			}
		}
		dst.Set(s)
		return nil
	}
	plan.toGen = func(src, gen reflect.Value) error {
		n := src.Len()
		if n == 0 {
			return nil
		}
		l := reflect.MakeSlice(gt, n, n)
		for i := range n {
			if err := gconv.toGen(src.Index(i), l.Index(i)); err != nil {
				return err
			}
		}
		gen.Field(access.index).Set(l)
		return nil
	}
	return plan, nil
}

// mapField converts a map field and a map.
func (p *planner) mapField(fd protoreflect.FieldDescriptor, t reflect.Type, access *genAccess) (fieldPlan, error) {
	if t.Kind() != reflect.Map {
		return fieldPlan{}, fmt.Errorf("a map field needs a map, not %s", t)
	}
	keyConv, err := p.scalar(fd.MapKey(), t.Key())
	if err != nil {
		return fieldPlan{}, fmt.Errorf("key: %w", err)
	}
	valueConv, err := p.element(fd.MapValue(), t.Elem())
	if err != nil {
		return fieldPlan{}, fmt.Errorf("value: %w", err)
	}
	plan := fieldPlan{
		toGo: func(m protoreflect.Message, dst reflect.Value) error {
			if !m.Has(fd) {
				return nil
			}
			pm := m.Get(fd).Map()
			gm := reflect.MakeMapWithSize(t, pm.Len())
			var err error
			pm.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
				key := reflect.New(t.Key()).Elem()
				if err = keyConv.toGo(k.Value(), key); err != nil {
					return false
				}
				value := reflect.New(t.Elem()).Elem()
				if err = valueConv.toGo(v, value); err != nil {
					return false
				}
				gm.SetMapIndex(key, value)
				return true
			})
			if err != nil {
				return err
			}
			dst.Set(gm)
			return nil
		},
		toProto: func(src reflect.Value, m protoreflect.Message) error {
			if src.Len() == 0 {
				return nil
			}
			pm := m.Mutable(fd).Map()
			for it := src.MapRange(); it.Next(); {
				k, err := keyConv.toProto(it.Key())
				if err != nil {
					return err
				}
				v, err := valueConv.toProto(it.Value())
				if err != nil {
					return err
				}
				pm.Set(k.MapKey(), v)
			}
			return nil
		},
	}
	if access == nil {
		return plan, nil
	}
	gt := access.typ
	gkey, err := p.genScalar(fd.MapKey(), t.Key())
	if err != nil {
		return fieldPlan{}, fmt.Errorf("key: %w", err)
	}
	gvalue, err := p.genElement(fd.MapValue(), gt.Elem(), t.Elem())
	if err != nil {
		return fieldPlan{}, fmt.Errorf("value: %w", err)
	}
	// SetMapIndex copies the key and the value, so one of each serves every entry.
	plan.fromGen = func(gen, dst reflect.Value) error {
		gm := gen.Field(access.index)
		if gm.Len() == 0 {
			return nil
		}
		m := reflect.MakeMapWithSize(t, gm.Len())
		key, value := reflect.New(t.Key()).Elem(), reflect.New(t.Elem()).Elem()
		for it := gm.MapRange(); it.Next(); {
			key.SetZero()
			value.SetZero()
			if err := gkey.toGo(it.Key(), key); err != nil {
				return err
			}
			if err := gvalue.toGo(it.Value(), value); err != nil {
				return err
			}
			m.SetMapIndex(key, value)
		}
		dst.Set(m)
		return nil
	}
	plan.toGen = func(src, gen reflect.Value) error {
		if src.Len() == 0 {
			return nil
		}
		gm := reflect.MakeMapWithSize(gt, src.Len())
		key, value := reflect.New(gt.Key()).Elem(), reflect.New(gt.Elem()).Elem()
		for it := src.MapRange(); it.Next(); {
			key.SetZero()
			value.SetZero()
			if err := gkey.toGen(it.Key(), key); err != nil {
				return err
			}
			if err := gvalue.toGen(it.Value(), value); err != nil {
				return err
			}
			gm.SetMapIndex(key, value)
		}
		gen.Field(access.index).Set(gm)
		return nil
	}
	return plan, nil
}

// valueConv converts a proto value, of a field, a list element or a map entry, and a Go value.
type valueConv struct {
	// toGo sets dst, which is addressable and holds its zero value.
	toGo    func(v protoreflect.Value, dst reflect.Value) error
	toProto func(src reflect.Value) (protoreflect.Value, error)
}

// element returns the conversion of the elements of a list or the values of a map, which may be pointers to
// the structs of messages.
func (p *planner) element(fd protoreflect.FieldDescriptor, t reflect.Type) (valueConv, error) {
	if t.Kind() != reflect.Pointer || isProtoPointer(t) {
		return p.value(fd, t)
	}
	if fd.Message() == nil {
		return valueConv{}, fmt.Errorf("cannot map %s to %s", fd.Kind(), t)
	}
	conv, err := p.value(fd, t.Elem())
	if err != nil {
		return valueConv{}, err
	}
	elem := t.Elem()
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			ptr := reflect.New(elem)
			if err := conv.toGo(v, ptr.Elem()); err != nil {
				return err
			}
			dst.Set(ptr)
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			if src.IsNil() {
				return protoreflect.Value{}, errNilElement
			}
			return conv.toProto(src.Elem())
		},
	}, nil
}

// value returns the conversion of a value of fd and a Go value of type t.
func (p *planner) value(fd protoreflect.FieldDescriptor, t reflect.Type) (valueConv, error) {
	if md := fd.Message(); md != nil {
		return p.messageValue(md, t)
	}
	return p.scalar(fd, t)
}

// messageValue converts a message and a Go value: the proto message itself, a Go type that stands for a
// well-known type, such as time.Time for google.protobuf.Timestamp, or a struct.
func (p *planner) messageValue(md protoreflect.MessageDescriptor, t reflect.Type) (valueConv, error) {
	if isProtoPointer(t) {
		return protoValue(md, t)
	}
	name := md.FullName()
	mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		return valueConv{}, fmt.Errorf("message %s is not in the proto registry", name)
	}
	switch {
	case name == "google.protobuf.Timestamp" && t == timeType:
		return timestampValue(md, mt), nil
	case name == "google.protobuf.Duration" && t == durationType:
		return durationValue(md, mt), nil
	case wrapperMessages[name] && t.Kind() != reflect.Struct:
		return p.wrapperValue(md, mt, t)
	case name == "google.protobuf.Struct" && t.Kind() == reflect.Map && t.Key().Kind() == reflect.String && t.Elem() == anyType:
		return structValue(mt, t), nil
	case name == "google.protobuf.Value" && t == anyType:
		return dynamicValue(mt), nil
	case name == "google.protobuf.ListValue" && t.Kind() == reflect.Slice && t.Elem() == anyType:
		return listValue(mt, t), nil
	case name == "google.protobuf.FieldMask" && t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.String:
		return fieldMaskValue(md, mt, t), nil
	case t.Kind() == reflect.Struct:
		plan, err := p.message(md, t)
		if err != nil {
			return valueConv{}, err
		}
		return valueConv{
			toGo: func(v protoreflect.Value, dst reflect.Value) error {
				return plan.toGo(v.Message(), dst)
			},
			toProto: func(src reflect.Value) (protoreflect.Value, error) {
				m, err := plan.toProto(src)
				if err != nil {
					return protoreflect.Value{}, err
				}
				return protoreflect.ValueOfMessage(m), nil
			},
		}, nil
	default:
		return valueConv{}, fmt.Errorf("cannot map message %s to %s", name, t)
	}
}

// isProtoPointer reports whether t is a pointer to a proto message.
func isProtoPointer(t reflect.Type) bool {
	return t.Kind() == reflect.Pointer && t.Implements(protoMessageType)
}

// protoValue passes a message of md as it is, as t.
func protoValue(md protoreflect.MessageDescriptor, t reflect.Type) (valueConv, error) {
	if name := reflect.New(t.Elem()).Interface().(proto.Message).ProtoReflect().Descriptor().FullName(); name != md.FullName() {
		return valueConv{}, fmt.Errorf("%s is %s", t, name)
	}
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			m := reflect.ValueOf(v.Message().Interface())
			if m.Type() != t {
				// another implementation of the message, such as a dynamicpb.Message
				m = reflect.New(t.Elem())
				proto.Merge(m.Interface().(proto.Message), v.Message().Interface())
			}
			dst.Set(m)
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			if src.IsNil() {
				return protoreflect.Value{}, errNilElement
			}
			return protoreflect.ValueOfMessage(src.Interface().(proto.Message).ProtoReflect()), nil
		},
	}, nil
}

// timestampValue converts a google.protobuf.Timestamp and a time.Time, in UTC.
func timestampValue(md protoreflect.MessageDescriptor, mt protoreflect.MessageType) valueConv {
	seconds, nanos := md.Fields().ByName("seconds"), md.Fields().ByName("nanos")
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			m := v.Message()
			t, err := timeOf(m.Get(seconds).Int(), m.Get(nanos).Int())
			if err != nil {
				return err
			}
			setAs(dst, t)
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			s, n, err := timestampOf(valueAs[time.Time](src))
			if err != nil {
				return protoreflect.Value{}, err
			}
			m := mt.New()
			m.Set(seconds, protoreflect.ValueOfInt64(s))
			m.Set(nanos, protoreflect.ValueOfInt32(n))
			return protoreflect.ValueOfMessage(m), nil
		},
	}
}

// timeOf returns the time of a google.protobuf.Timestamp, in UTC.
func timeOf(s, n int64) (time.Time, error) {
	if s < minTimestampSeconds || s > maxTimestampSeconds || n < 0 || n >= int64(time.Second) {
		return time.Time{}, fmt.Errorf("invalid timestamp: %d seconds, %d nanoseconds", s, n)
	}
	return time.Unix(s, n).UTC(), nil
}

// timestampOf returns the seconds and nanoseconds of the google.protobuf.Timestamp of t.
func timestampOf(t time.Time) (int64, int32, error) {
	s := t.Unix()
	if s < minTimestampSeconds || s > maxTimestampSeconds {
		return 0, 0, fmt.Errorf("%s is out of the range of google.protobuf.Timestamp", t)
	}
	return s, int32(t.Nanosecond()), nil
}

// durationValue converts a google.protobuf.Duration and a time.Duration.
func durationValue(md protoreflect.MessageDescriptor, mt protoreflect.MessageType) valueConv {
	seconds, nanos := md.Fields().ByName("seconds"), md.Fields().ByName("nanos")
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			m := v.Message()
			d, err := durationOf(m.Get(seconds).Int(), m.Get(nanos).Int())
			if err != nil {
				return err
			}
			dst.SetInt(int64(d))
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			d := time.Duration(src.Int())
			m := mt.New()
			m.Set(seconds, protoreflect.ValueOfInt64(int64(d/time.Second)))
			m.Set(nanos, protoreflect.ValueOfInt32(int32(d%time.Second)))
			return protoreflect.ValueOfMessage(m), nil
		},
	}
}

// durationOf returns the time.Duration of a google.protobuf.Duration, which fails when it does not fit.
func durationOf(s, n int64) (time.Duration, error) {
	d := time.Duration(s) * time.Second
	if s > math.MaxInt64/int64(time.Second) || s < math.MinInt64/int64(time.Second) ||
		n <= -int64(time.Second) || n >= int64(time.Second) || (s > 0 && n < 0) || (s < 0 && n > 0) {
		return 0, fmt.Errorf("invalid duration: %d seconds, %d nanoseconds", s, n)
	}
	sum := d + time.Duration(n)
	if (n > 0 && sum < d) || (n < 0 && sum > d) {
		return 0, fmt.Errorf("invalid duration: %d seconds, %d nanoseconds", s, n)
	}
	return sum, nil
}

// wrapperValue converts a wrapper message, such as google.protobuf.StringValue, like its value field.
func (p *planner) wrapperValue(md protoreflect.MessageDescriptor, mt protoreflect.MessageType, t reflect.Type) (valueConv, error) {
	fd := md.Fields().ByName("value")
	inner, err := p.scalar(fd, t)
	if err != nil {
		return valueConv{}, err
	}
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			return inner.toGo(v.Message().Get(fd), dst)
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			v, err := inner.toProto(src)
			if err != nil {
				return protoreflect.Value{}, err
			}
			m := mt.New()
			m.Set(fd, v)
			return protoreflect.ValueOfMessage(m), nil
		},
	}, nil
}

// asGenerated returns m as the generated message of its type, a new one when m is another implementation.
func asGenerated[M proto.Message](m protoreflect.Message, empty M) M {
	if g, ok := m.Interface().(M); ok {
		return g
	}
	proto.Merge(empty, m.Interface())
	return empty
}

// fromGenerated returns m as a message of mt, the type of the field it goes in.
func fromGenerated(mt protoreflect.MessageType, m proto.Message) protoreflect.Value {
	if m.ProtoReflect().Type() == mt {
		return protoreflect.ValueOfMessage(m.ProtoReflect())
	}
	dst := mt.New()
	proto.Merge(dst.Interface(), m)
	return protoreflect.ValueOfMessage(dst)
}

// structValue converts a google.protobuf.Struct and a map of string to any, as structpb does.
func structValue(mt protoreflect.MessageType, t reflect.Type) valueConv {
	mapType := reflect.TypeFor[map[string]any]()
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			dst.Set(reflect.ValueOf(asGenerated(v.Message(), &structpb.Struct{}).AsMap()).Convert(t))
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			s, err := structpb.NewStruct(src.Convert(mapType).Interface().(map[string]any))
			if err != nil {
				return protoreflect.Value{}, err
			}
			return fromGenerated(mt, s), nil
		},
	}
}

// dynamicValue converts a google.protobuf.Value and an any, as structpb does.
func dynamicValue(mt protoreflect.MessageType) valueConv {
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			if x := asGenerated(v.Message(), &structpb.Value{}).AsInterface(); x != nil {
				dst.Set(reflect.ValueOf(x))
			}
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			v, err := structpb.NewValue(src.Interface())
			if err != nil {
				return protoreflect.Value{}, err
			}
			return fromGenerated(mt, v), nil
		},
	}
}

// listValue converts a google.protobuf.ListValue and a slice of any, as structpb does.
func listValue(mt protoreflect.MessageType, t reflect.Type) valueConv {
	sliceType := reflect.TypeFor[[]any]()
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			dst.Set(reflect.ValueOf(asGenerated(v.Message(), &structpb.ListValue{}).AsSlice()).Convert(t))
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			l, err := structpb.NewList(src.Convert(sliceType).Interface().([]any))
			if err != nil {
				return protoreflect.Value{}, err
			}
			return fromGenerated(mt, l), nil
		},
	}
}

// fieldMaskValue converts a google.protobuf.FieldMask and a slice of its paths.
func fieldMaskValue(md protoreflect.MessageDescriptor, mt protoreflect.MessageType, t reflect.Type) valueConv {
	paths := md.Fields().ByName("paths")
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			l := v.Message().Get(paths).List()
			s := reflect.MakeSlice(t, l.Len(), l.Len())
			for i := range l.Len() {
				s.Index(i).SetString(l.Get(i).String())
			}
			dst.Set(s)
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			m := mt.New()
			l := m.Mutable(paths).List()
			for i := range src.Len() {
				l.Append(protoreflect.ValueOfString(src.Index(i).String()))
			}
			return protoreflect.ValueOfMessage(m), nil
		},
	}
}

// scalar returns the conversion of a scalar or enum value of fd and a Go value of type t.
func (p *planner) scalar(fd protoreflect.FieldDescriptor, t reflect.Type) (valueConv, error) {
	switch kind := fd.Kind(); kind {
	case protoreflect.StringKind:
		if conv, ok := p.text(t); ok {
			return conv, nil
		}
		if t.Kind() == reflect.String {
			return valueConv{
				toGo: func(v protoreflect.Value, dst reflect.Value) error {
					dst.SetString(v.String())
					return nil
				},
				toProto: func(src reflect.Value) (protoreflect.Value, error) {
					return protoreflect.ValueOfString(src.String()), nil
				},
			}, nil
		}
	case protoreflect.BytesKind:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return valueConv{
				toGo: func(v protoreflect.Value, dst reflect.Value) error {
					dst.SetBytes(v.Bytes())
					return nil
				},
				toProto: func(src reflect.Value) (protoreflect.Value, error) {
					return protoreflect.ValueOfBytes(src.Bytes()), nil
				},
			}, nil
		}
	case protoreflect.BoolKind:
		if t.Kind() == reflect.Bool {
			return valueConv{
				toGo: func(v protoreflect.Value, dst reflect.Value) error {
					dst.SetBool(v.Bool())
					return nil
				},
				toProto: func(src reflect.Value) (protoreflect.Value, error) {
					return protoreflect.ValueOfBool(src.Bool()), nil
				},
			}, nil
		}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		if t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64 {
			return floatValue(kind), nil
		}
	case protoreflect.EnumKind:
		if t.Kind() == reflect.String {
			return enumNameValue(fd.Enum()), nil
		}
		if isInteger(t) {
			return integerValue(kind), nil
		}
	default: // the integer kinds
		if isInteger(t) {
			return integerValue(kind), nil
		}
	}
	return valueConv{}, fmt.Errorf("cannot map %s to %s", fd.Kind(), t)
}

// text converts a string and a Go type that implements encoding.TextUnmarshaler, for a request, or
// encoding.TextMarshaler, for a response, such as a UUID type. The empty string is the zero value.
func (p *planner) text(t reflect.Type) (valueConv, bool) {
	if p.decode {
		if !reflect.PointerTo(t).Implements(textUnmarshalerType) {
			return valueConv{}, false
		}
		return valueConv{toGo: func(v protoreflect.Value, dst reflect.Value) error {
			s := v.String()
			if s == "" {
				return nil
			}
			return dst.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s))
		}}, true
	}

	byValue := t.Implements(textMarshalerType)
	if !byValue && !reflect.PointerTo(t).Implements(textMarshalerType) {
		return valueConv{}, false
	}
	return valueConv{toProto: func(src reflect.Value) (protoreflect.Value, error) {
		if src.IsZero() {
			return protoreflect.ValueOfString(""), nil
		}
		var m encoding.TextMarshaler
		switch {
		case byValue:
			m = src.Interface().(encoding.TextMarshaler)
		case src.CanAddr():
			m = src.Addr().Interface().(encoding.TextMarshaler)
		default:
			c := reflect.New(t)
			c.Elem().Set(src)
			m = c.Interface().(encoding.TextMarshaler)
		}
		b, err := m.MarshalText()
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfString(string(b)), nil
	}}, true
}

func isInteger(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}

func isSigned(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	}
	return false
}

// integerValue converts an integer or enum of kind and any Go integer type. A value out of the range of
// its destination fails.
func integerValue(kind protoreflect.Kind) valueConv {
	var signed bool
	var bits int
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.EnumKind:
		signed, bits = true, 32
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		signed, bits = true, 64
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		bits = 32
	default:
		bits = 64
	}
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			switch {
			case kind == protoreflect.EnumKind:
				return setInt(dst, int64(v.Enum()))
			case signed:
				return setInt(dst, v.Int())
			default:
				return setUint(dst, v.Uint())
			}
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			if signed {
				var n int64
				if isSigned(src.Type()) {
					n = src.Int()
				} else {
					u := src.Uint()
					if u > math.MaxInt64 {
						return protoreflect.Value{}, outOfRange(u, kind)
					}
					n = int64(u)
				}
				if bits == 32 && (n < math.MinInt32 || n > math.MaxInt32) {
					return protoreflect.Value{}, outOfRange(n, kind)
				}
				switch {
				case kind == protoreflect.EnumKind:
					return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
				case bits == 32:
					return protoreflect.ValueOfInt32(int32(n)), nil
				default:
					return protoreflect.ValueOfInt64(n), nil
				}
			}
			var u uint64
			if isSigned(src.Type()) {
				n := src.Int()
				if n < 0 {
					return protoreflect.Value{}, outOfRange(n, kind)
				}
				u = uint64(n)
			} else {
				u = src.Uint()
			}
			if bits == 32 {
				if u > math.MaxUint32 {
					return protoreflect.Value{}, outOfRange(u, kind)
				}
				return protoreflect.ValueOfUint32(uint32(u)), nil
			}
			return protoreflect.ValueOfUint64(u), nil
		},
	}
}

// setInt sets dst, of any integer type, to n.
func setInt(dst reflect.Value, n int64) error {
	if isSigned(dst.Type()) {
		if dst.OverflowInt(n) {
			return outOfRange(n, dst.Type())
		}
		dst.SetInt(n)
		return nil
	}
	if n < 0 || dst.OverflowUint(uint64(n)) {
		return outOfRange(n, dst.Type())
	}
	dst.SetUint(uint64(n))
	return nil
}

// setUint sets dst, of any integer type, to u.
func setUint(dst reflect.Value, u uint64) error {
	if isSigned(dst.Type()) {
		if u > math.MaxInt64 || dst.OverflowInt(int64(u)) {
			return outOfRange(u, dst.Type())
		}
		dst.SetInt(int64(u))
		return nil
	}
	if dst.OverflowUint(u) {
		return outOfRange(u, dst.Type())
	}
	dst.SetUint(u)
	return nil
}

func outOfRange(v any, target any) error {
	return fmt.Errorf("%v is out of the range of %v", v, target)
}

// floatValue converts a float or double and a Go float type.
func floatValue(kind protoreflect.Kind) valueConv {
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			f := v.Float()
			if dst.OverflowFloat(f) {
				return outOfRange(f, dst.Type())
			}
			dst.SetFloat(f)
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			if kind == protoreflect.FloatKind {
				f := src.Float()
				if !math.IsInf(f, 0) && math.Abs(f) > math.MaxFloat32 {
					return protoreflect.Value{}, outOfRange(f, kind)
				}
				return protoreflect.ValueOfFloat32(float32(f)), nil
			}
			return protoreflect.ValueOfFloat64(src.Float()), nil
		},
	}
}

// enumNameValue converts an enum and a Go string type holding the names of its values. The empty string is
// the value 0.
func enumNameValue(ed protoreflect.EnumDescriptor) valueConv {
	values := ed.Values()
	return valueConv{
		toGo: func(v protoreflect.Value, dst reflect.Value) error {
			ev := values.ByNumber(v.Enum())
			if ev == nil {
				return fmt.Errorf("%d is not a value of %s", v.Enum(), ed.FullName())
			}
			dst.SetString(string(ev.Name()))
			return nil
		},
		toProto: func(src reflect.Value) (protoreflect.Value, error) {
			name := src.String()
			if name == "" {
				return protoreflect.ValueOfEnum(0), nil
			}
			ev := values.ByName(protoreflect.Name(name))
			if ev == nil {
				return protoreflect.Value{}, fmt.Errorf("%q is not a value of %s", name, ed.FullName())
			}
			return protoreflect.ValueOfEnum(ev.Number()), nil
		},
	}
}

// headerPlan sets a field of a request from the values of a metadata key.
type headerPlan struct {
	key   string
	index []int
	set   func(values []string, dst reflect.Value) error
}

// headerPlans returns the plans of the fields of t tagged header, which come from the call's metadata as
// they come from the request headers over HTTP.
func headerPlans(t reflect.Type, fields []goField) ([]headerPlan, error) {
	var plans []headerPlan
	for _, f := range fields {
		if f.header == "" || f.exact != "" {
			continue
		}
		set, err := headerSetter(f.sf.Type)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: metadata %s: %w", t, f.sf.Name, f.header, err)
		}
		plans = append(plans, headerPlan{key: f.header, index: f.sf.Index, set: set})
	}
	return plans, nil
}

// headerSetter returns the function that sets a field of type t from metadata values: a slice of strings
// takes every value, and any other type the first one. A pointer stays nil without values.
func headerSetter(t reflect.Type) (func([]string, reflect.Value) error, error) {
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.String {
		return func(values []string, dst reflect.Value) error {
			s := reflect.MakeSlice(t, len(values), len(values))
			for i, v := range values {
				s.Index(i).SetString(v)
			}
			dst.Set(s)
			return nil
		}, nil
	}
	if t.Kind() == reflect.Pointer {
		set, err := headerSetter(t.Elem())
		if err != nil {
			return nil, err
		}
		return func(values []string, dst reflect.Value) error {
			v := reflect.New(t.Elem())
			if err := set(values, v.Elem()); err != nil {
				return err
			}
			dst.Set(v)
			return nil
		}, nil
	}
	parse, err := stringParser(t)
	if err != nil {
		return nil, err
	}
	return func(values []string, dst reflect.Value) error {
		return parse(values[0], dst)
	}, nil
}

// stringParser returns the function that sets a value of type t from a string.
func stringParser(t reflect.Type) (func(string, reflect.Value) error, error) {
	if reflect.PointerTo(t).Implements(textUnmarshalerType) {
		return func(s string, dst reflect.Value) error {
			return dst.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(s))
		}, nil
	}
	switch t.Kind() {
	case reflect.String:
		return func(s string, dst reflect.Value) error {
			dst.SetString(s)
			return nil
		}, nil
	case reflect.Bool:
		return func(s string, dst reflect.Value) error {
			b, err := strconv.ParseBool(s)
			dst.SetBool(b)
			return err
		}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(s string, dst reflect.Value) error {
			n, err := strconv.ParseInt(s, 10, t.Bits())
			dst.SetInt(n)
			return err
		}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(s string, dst reflect.Value) error {
			n, err := strconv.ParseUint(s, 10, t.Bits())
			dst.SetUint(n)
			return err
		}, nil
	case reflect.Float32, reflect.Float64:
		return func(s string, dst reflect.Value) error {
			f, err := strconv.ParseFloat(s, t.Bits())
			dst.SetFloat(f)
			return err
		}, nil
	}
	return nil, fmt.Errorf("cannot set %s from metadata", t)
}
