package grpc

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/server/grpc/internal/gentest"

	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The direct conversions must agree with the protoreflect ones on every message: the tests below convert
// generated messages both ways and compare the results, errors included.

type kindsPoint struct{ X, Y int }

// kindsStruct is the business struct of forgo.gentest.Kinds, a proto2 message.
type kindsStruct struct {
	Name     *string
	Count    int32
	Big      uint64
	Ratio    float64
	On       *bool
	Data     []byte
	Color    string
	ID       string
	Colors   []string
	Flags    map[bool]string
	IDs      map[uint64]int
	Signed   map[int]string
	Nested   map[uint32]kindsStruct // by value: each entry starts from the zero value
	Child    *kindsStruct
	Children []kindsStruct
	Point    *kindsPoint
	Text     *string
	Number   *int64
	Blob     *[]byte
	Sub      *kindsStruct
	Shade    *string
	When     *time.Time
	Doubles  []float32
	Blobs    [][]byte
	Stamp    time.Time
	Wait     time.Duration
	Maybe    *bool
	Stamps   []time.Time
	Sizes    []int64
}

// levelsStruct is the business struct of forgo.gentest.Hybrid and forgo.gentest.Opaque.
type levelsStruct struct {
	Name   string
	N      *int32
	Tags   []string
	Counts map[string]int
	Text   *string
	Child  *levelsStruct
	Sub    *levelsStruct
	Data   []byte
}

// mixedStruct is the business struct of forgo.gentest.Mixed, which holds messages of the opaque API.
type mixedStruct struct {
	Label  string
	One    *levelsStruct
	Many   []levelsStruct
	ByKey  map[string]*levelsStruct
	Either *levelsStruct
	Word   *string
}

// bindingPair returns the bindings of T and md: direct, and through protoreflect.
func bindingPair[T any](t testing.TB, md protoreflect.MessageDescriptor, decode bool) [2]binding[T] {
	t.Helper()
	var pair [2]binding[T]
	for i, direct := range []bool{true, false} {
		b, err := newBindingVia[T](md, decode, direct)
		if err != nil {
			t.Fatal(err)
		}
		pair[i] = b
	}
	return pair
}

// checkDecode converts msg with both bindings, fails unless they agree, and returns the result.
func checkDecode[T any](t testing.TB, pair [2]binding[T], msg proto.Message) (T, bool) {
	t.Helper()
	got, gotErr := pair[0].toGo(context.Background(), msg)
	want, wantErr := pair[1].toGo(context.Background(), msg)
	if !sameError(gotErr, wantErr) {
		t.Errorf("decode %v: direct %v, protoreflect %v", msg, gotErr, wantErr)
		return got, false
	}
	if gotErr == nil && !sameValue(reflect.ValueOf(got), reflect.ValueOf(want)) {
		t.Errorf("decode %v:\ndirect       %+v\nprotoreflect %+v", msg, got, want)
	}
	return got, gotErr == nil
}

// checkEncode converts v with both bindings, fails unless they agree, and reports whether they succeeded.
func checkEncode[T any](t testing.TB, pair [2]binding[T], v T) bool {
	t.Helper()
	got, gotErr := pair[0].toProto(v)
	want, wantErr := pair[1].toProto(v)
	if !sameError(gotErr, wantErr) {
		t.Errorf("encode %+v: direct %v, protoreflect %v", v, gotErr, wantErr)
		return false
	}
	if gotErr != nil {
		return false
	}
	if reflect.TypeOf(got) != reflect.TypeOf(want) {
		t.Fatalf("encode %+v: direct %T, protoreflect %T", v, got, want)
	}
	opts := proto.MarshalOptions{AllowPartial: true, Deterministic: true}
	g, gErr := opts.Marshal(got)
	w, wErr := opts.Marshal(want)
	if string(g) != string(w) || (gErr == nil) != (wErr == nil) {
		t.Errorf("encode %+v:\ndirect       %v (%v)\nprotoreflect %v (%v)", v, got, gErr, want, wErr)
	}
	return true
}

// mustAgree fails unless ok, the result of checkDecode or checkEncode on a value both convert.
func mustAgree(t testing.TB, ok bool, what string) {
	t.Helper()
	if !ok {
		t.Fatalf("%s did not convert", what)
	}
}

// sameError reports whether a and b are the same failure: the same field, for a fieldError, which the
// order of a map's entries can make another value of; the same text otherwise.
func sameError(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var fa, fb *fieldError
	if errors.As(a, &fa) && errors.As(b, &fb) {
		return fa.source == fb.source && fa.Path() == fb.Path()
	}
	return a.Error() == b.Error()
}

// sameValue is reflect.DeepEqual, except that NaN equals NaN and messages compare with proto.Equal.
func sameValue(a, b reflect.Value) bool {
	if !a.IsValid() || !b.IsValid() {
		return a.IsValid() == b.IsValid()
	}
	if a.Type() != b.Type() {
		return false
	}
	if a.Type().Implements(protoMessageType) && a.CanInterface() {
		return proto.Equal(a.Interface().(proto.Message), b.Interface().(proto.Message))
	}
	switch a.Kind() {
	case reflect.Float32, reflect.Float64:
		x, y := a.Float(), b.Float()
		return x == y && math.Signbit(x) == math.Signbit(y) || math.IsNaN(x) && math.IsNaN(y)
	case reflect.Pointer, reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return a.IsNil() == b.IsNil()
		}
		return sameValue(a.Elem(), b.Elem())
	case reflect.Slice:
		if a.IsNil() != b.IsNil() {
			return false
		}
		fallthrough
	case reflect.Array:
		if a.Len() != b.Len() {
			return false
		}
		for i := range a.Len() {
			if !sameValue(a.Index(i), b.Index(i)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if a.IsNil() != b.IsNil() || a.Len() != b.Len() {
			return false
		}
		for it := a.MapRange(); it.Next(); {
			if v := b.MapIndex(it.Key()); !v.IsValid() || !sameValue(it.Value(), v) {
				return false
			}
		}
		return true
	case reflect.Struct:
		for i := range a.NumField() {
			if !sameValue(a.Field(i), b.Field(i)) {
				return false
			}
		}
		return true
	case reflect.Bool:
		return a.Bool() == b.Bool()
	case reflect.String:
		return a.String() == b.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return a.Int() == b.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return a.Uint() == b.Uint()
	}
	panic("sameValue: unexpected kind " + a.Kind().String())
}

func descriptorOf(m proto.Message) protoreflect.MessageDescriptor {
	return m.ProtoReflect().Descriptor()
}

func genUser(t testing.TB, js string) *gentest.User {
	t.Helper()
	m := new(gentest.User)
	if err := protojson.Unmarshal([]byte(js), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// The bindings of generated messages of the open and hybrid APIs convert directly, and the others through
// protoreflect.
func TestDirectBindingApplies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		md     protoreflect.MessageDescriptor
		decode func(protoreflect.MessageDescriptor) (bool, error)
		direct bool
	}{
		{"open", descriptorOf(&gentest.User{}), directOf[user], true},
		{"proto2", descriptorOf(&gentest.Kinds{}), directOf[kindsStruct], true},
		{"hybrid", descriptorOf(&gentest.Hybrid{}), directOf[levelsStruct], true},
		{"opaque", descriptorOf(&gentest.Opaque{}), directOf[levelsStruct], false},
		{"open holding opaque", descriptorOf(&gentest.Mixed{}), directOf[mixedStruct], true},
		{"dynamicpb", message("User"), directOf[user], false},
		{"grpc_testing", descriptorOf(&testpb.SimpleRequest{}), directOf[echoRequest], true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if direct, err := tc.decode(tc.md); err != nil || direct != tc.direct {
				t.Fatalf("direct %v, err %v; want %v", direct, err, tc.direct)
			}
		})
	}
}

func directOf[T any](md protoreflect.MessageDescriptor) (bool, error) {
	b, err := newBinding[T](md, true)
	return b.direct, err
}

func TestDirectBindingUsers(t *testing.T) {
	users := bindingPair[user](t, descriptorOf(&gentest.User{}), true)
	full := genUser(t, fullUserJSON)
	nan, negZero := math.NaN(), math.Copysign(0, -1)
	// A message of another implementation goes through protoreflect.
	dynamic := dynamicpb.NewMessage(descriptorOf(full))
	proto.Merge(dynamic, full)
	checkDecode(t, users, dynamic)
	if got, err := users[0].toGo(context.Background(), dynamic); err != nil || got.Name != "Ada" {
		t.Fatalf("decoded %+v, %v", got, err)
	}

	_, ok := checkDecode(t, users, full)
	mustAgree(t, ok, "the full user")
	for _, msg := range []*gentest.User{
		{},
		full,
		{Score: nan, Avatar: []byte{}, Nick: proto.String(""), Contact: &gentest.User_Email{}},
		{Score: negZero, Contact: &gentest.User_Phone{Phone: "1"}},
		{Contact: (*gentest.User_Email)(nil)},
		{Previous: []*gentest.Address{nil, {Street: "a"}}, Counts: map[int32]*gentest.Address{3: nil, 4: {}}},
		{Manager: &gentest.User{Manager: &gentest.User{Name: "c"}}},
		{Extra: structpb.NewNullValue(), Attributes: &structpb.Struct{}, Mask: &fieldmaskpb.FieldMask{}},
		{Nickname: wrapperspb.String(""), Created: &timestamppb.Timestamp{}},
		{Age: 1000},
		{Age: -129},
		{Status: 99},
		{Level: 3, Id: "u-1"},
		{Id: "x"},
		{CreatedAt: &timestamppb.Timestamp{Seconds: maxTimestampSeconds + 1}},
		{CreatedAt: &timestamppb.Timestamp{Nanos: -1}},
		{Ttl: &durationpb.Duration{Seconds: math.MaxInt64}},
		{Ttl: &durationpb.Duration{Seconds: 1, Nanos: -1}},
		{Big: math.MaxUint64, Labels: map[string]string{"": "", "a": "b"}},
	} {
		checkDecode(t, users, msg)
	}

	encode := bindingPair[user](t, descriptorOf(&gentest.User{}), false)
	if !encode[0].direct {
		t.Fatal("the response binding is not direct")
	}
	mustAgree(t, checkEncode(t, encode, fullUser()), "the full user")
	for _, u := range []user{
		{},
		fullUser(),
		{Email: ptr("a"), Phone: ptr("b")},
		{Status: "NOPE"},
		{CreatedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Counts: map[int]*address{1: nil}},
		{Counts: map[int]*address{1: {}, 2: {Street: "s"}}},
		{Attributes: map[string]any{"c": make(chan int)}},
		{Extra: make(chan int)},
		{Extra: []any{1.0, "x", nil}},
		{Avatar: []byte{}, Nickname: ptr(""), Nick: ptr(""), Score: negZero, Mask: []string{}},
		{Manager: &user{Manager: &user{Name: "c"}}},
		{Created: ptr(time.Time{})},
	} {
		checkEncode(t, encode, u)
	}
}

// userPresence reads the implicit scalars of forgo.gentest.User through pointers, which stay nil for the
// fields that are not set: their zero value.
type userPresence struct {
	Name   *string
	Age    *int
	Score  *float64
	Active *bool
	Avatar *[]byte
	Status *string
	Big    *uint64
	_      struct{} `grpc:"id"`
	_      struct{} `grpc:"level"`
	_      struct{} `grpc:"address"`
	_      struct{} `grpc:"previous"`
	_      struct{} `grpc:"tags"`
	_      struct{} `grpc:"labels"`
	_      struct{} `grpc:"created_at"`
	_      struct{} `grpc:"ttl"`
	_      struct{} `grpc:"nickname"`
	_      struct{} `grpc:"attributes"`
	_      struct{} `grpc:"extra"`
	_      struct{} `grpc:"mask"`
	_      struct{} `grpc:"email"`
	_      struct{} `grpc:"phone"`
	_      struct{} `grpc:"manager"`
	_      struct{} `grpc:"nick"`
	_      struct{} `grpc:"counts"`
	_      struct{} `grpc:"created"`
}

func TestDirectBindingPresence(t *testing.T) {
	pair := bindingPair[userPresence](t, descriptorOf(&gentest.User{}), true)
	for _, msg := range []*gentest.User{
		{},
		{Name: "a", Age: 1, Score: 0.5, Active: true, Avatar: []byte{1}, Status: gentest.Status_STATUS_ACTIVE, Big: 1},
		{Age: -1, Score: math.Copysign(0, -1), Avatar: []byte{}},
	} {
		got, ok := checkDecode(t, pair, msg)
		mustAgree(t, ok, "the user")
		if msg.Active != (got.Active != nil) || (msg.Name == "") != (got.Name == nil) {
			t.Fatalf("decoded %+v from %v", got, msg)
		}
	}
	encode := bindingPair[userPresence](t, descriptorOf(&gentest.User{}), false)
	for _, v := range []userPresence{{}, {Name: ptr(""), Active: ptr(false), Avatar: new([]byte), Status: ptr("")}} {
		mustAgree(t, checkEncode(t, encode, v), "the user")
	}
}

// upperText is a string type with text methods of its own, which convert it rather than its string.
type upperText string

func (u upperText) MarshalText() ([]byte, error) { return []byte(strings.ToUpper(string(u))), nil }

func (u *upperText) UnmarshalText(b []byte) error {
	*u = upperText(strings.ToLower(string(b)))
	return nil
}

func TestDirectBindingText(t *testing.T) {
	type textAddress struct {
		Street  upperText
		ZipCode string
	}
	decode := bindingPair[textAddress](t, descriptorOf(&gentest.Address{}), true)
	got, ok := checkDecode(t, decode, &gentest.Address{Street: "MAIN", ZipCode: "Z"})
	mustAgree(t, ok, "the address")
	if got.Street != "main" {
		t.Fatalf("decoded %+v", got)
	}
	mustAgree(t, checkEncode(t, bindingPair[textAddress](t, descriptorOf(&gentest.Address{}), false),
		textAddress{Street: "main"}), "the address")
}

func TestDirectBindingScalars(t *testing.T) {
	decode := bindingPair[scalars](t, descriptorOf(&gentest.Scalars{}), true)
	for i, msg := range []*gentest.Scalars{
		{},
		{I32: -1, I64: 127, U32: math.MaxUint32, U64: 255, S32: -32768, S64: math.MaxInt64, F32: 1, F64: 2,
			Sf32: -3, Sf64: -4, Fl: 1.5, Db: 2.5, List: must(structpb.NewList([]any{1, "a"})),
			Count: wrapperspb.Int64(-5), Stamp: timestamppb.New(time.Unix(1, 2)), Names: map[string]string{"u-1": "ONE"}},
		{I64: 128},
		{U64: 256},
		{S64: -1},
		{S32: math.MaxInt32},
		{Db: 1e300},
		{Fl: float32(math.Inf(1)), Db: math.NaN()},
		{Count: wrapperspb.Int64(math.MaxInt64)},
		{Names: map[string]string{"x": "y"}},
	} {
		if _, ok := checkDecode(t, decode, msg); i < 2 {
			mustAgree(t, ok, "scalars")
		}
	}

	encode := bindingPair[scalars](t, descriptorOf(&gentest.Scalars{}), false)
	for i, s := range []scalars{
		{},
		{I32: -1, I64: 127, U32: math.MaxUint32, U64: 255, S32: -5, S64: 6, F32: 7, F64: 8, Sf32: -9, Sf64: -10,
			Fl: 1.5, Db: 2.5, List: []any{1.0, "a"}, Count: 11, Stamp: timestamppb.New(time.Unix(1, 0)),
			Names: map[userID]ptrText{{1}: {"one"}}},
		{I32: math.MaxInt32 + 1},
		{U32: math.MaxUint32 + 1},
		{S64: math.MaxInt64 + 1},
		{Fl: float32(math.NaN()), Db: float32(math.Inf(-1))},
		{List: []any{make(chan int)}},
	} {
		if ok := checkEncode(t, encode, s); i < 2 {
			mustAgree(t, ok, "scalars")
		}
	}

	reports := bindingPair[importReport](t, descriptorOf(&gentest.ImportReport{}), true)
	for _, msg := range []*gentest.ImportReport{{}, {Imported: 2, Ids: []string{"u-1", "u-2"}}, {Ids: []string{"x"}}} {
		checkDecode(t, reports, msg)
	}
	mustAgree(t, checkEncode(t, bindingPair[importReport](t, descriptorOf(&gentest.ImportReport{}), false),
		importReport{Imported: 1, IDs: []userID{{1}, {2}}}), "the report")

	requests := bindingPair[getUser](t, descriptorOf(&gentest.GetUserRequest{}), true)
	for _, msg := range []*gentest.GetUserRequest{{}, {Id: "u-3", Fields: &fieldmaskpb.FieldMask{Paths: []string{"a", "b"}}}} {
		checkDecode(t, requests, msg)
	}
}

// kindsMessage is a forgo.gentest.Kinds with every field set.
func kindsMessage() *gentest.Kinds {
	return &gentest.Kinds{
		Name: proto.String("n"), Count: proto.Int32(3), Big: proto.Uint64(math.MaxUint64), Ratio: proto.Float32(1.5),
		On: proto.Bool(true), Data: []byte{1}, Color: gentest.Color_GREEN.Enum(), Id: proto.String("k"),
		Colors: []gentest.Color{gentest.Color_BLUE, gentest.Color_RED},
		Flags:  map[bool]string{true: "t", false: "f"}, Ids: map[uint64]int32{math.MaxUint64: 1, 0: 2},
		Signed: map[int32]gentest.Color{-1: gentest.Color_GREEN},
		Nested: map[uint32]*gentest.Kinds{7: {Name: proto.String("x"), Count: proto.Int32(1)}, 8: nil, 9: {On: proto.Bool(true)}},
		Child:  &gentest.Kinds{Child: &gentest.Kinds{}}, Children: []*gentest.Kinds{nil, {On: proto.Bool(false)}},
		Point: &gentest.Kinds_Point{X: proto.Int32(1)}, Choice: &gentest.Kinds_Number{Number: -5},
		Doubles: []float64{math.NaN(), math.Copysign(0, -1), 1.5}, Blobs: [][]byte{nil, {}, {1}},
		Stamp: timestamppb.New(time.Unix(5, 6)), Wait: durationpb.New(-time.Second), Maybe: wrapperspb.Bool(false),
		Stamps: []*timestamppb.Timestamp{nil, timestamppb.New(time.Unix(1, 0))}, Sizes: []*wrapperspb.Int64Value{nil, wrapperspb.Int64(3)},
	}
}

func TestDirectBindingKinds(t *testing.T) {
	decode := bindingPair[kindsStruct](t, descriptorOf(&gentest.Kinds{}), true)
	negZero := float32(math.Copysign(0, -1))
	for _, msg := range []*gentest.Kinds{
		{},
		kindsMessage(),
		// set to their zero value, with explicit presence
		{Name: proto.String(""), Count: proto.Int32(0), Big: proto.Uint64(0), Ratio: &negZero, On: proto.Bool(false),
			Data: []byte{}, Color: gentest.Color_RED.Enum()},
		{Choice: &gentest.Kinds_Text{}},
		{Choice: &gentest.Kinds_Blob{}},
		{Choice: &gentest.Kinds_Blob{Blob: []byte{}}},
		{Choice: &gentest.Kinds_Sub{}},
		{Choice: &gentest.Kinds_Sub{Sub: &gentest.Kinds{Name: proto.String("s")}}},
		{Choice: &gentest.Kinds_Shade{Shade: gentest.Color_BLUE}},
		{Choice: &gentest.Kinds_When{}},
		{Choice: &gentest.Kinds_When{When: timestamppb.New(time.Unix(9, 0))}},
		{Choice: (*gentest.Kinds_Text)(nil)},
		{Color: gentest.Color(9).Enum()},
		{Colors: []gentest.Color{9}},
		{Signed: map[int32]gentest.Color{1: 9}},
		{Choice: &gentest.Kinds_Shade{Shade: 9}},
		{Ids: map[uint64]int32{1: math.MaxInt32}},
		{Stamp: &timestamppb.Timestamp{Seconds: minTimestampSeconds - 1}},
		{Wait: &durationpb.Duration{Seconds: -1, Nanos: 1}},
		{Stamps: []*timestamppb.Timestamp{{Nanos: 1e9}}},
		{Ratio: proto.Float32(float32(math.NaN()))},
		{Doubles: []float64{1e300}},
	} {
		checkDecode(t, decode, msg)
	}

	encode := bindingPair[kindsStruct](t, descriptorOf(&gentest.Kinds{}), false)
	full, ok := checkDecode(t, decode, kindsMessage())
	if !ok || len(full.Nested) != 3 || full.Nested[9].Name != nil {
		t.Fatalf("decoded %+v", full)
	}
	mustAgree(t, checkEncode(t, encode, full), "the full kinds")
	for _, k := range []kindsStruct{
		{},
		full,
		{Data: []byte{}, Name: ptr(""), On: ptr(false), Color: "", Count: 0},
		{Color: "GREEN", Colors: []string{"RED", "", "BLUE"}, Signed: map[int]string{-3: "BLUE"}},
		{Text: ptr("")},
		{Blob: new([]byte)},
		{Blob: ptr([]byte{})},
		{Sub: &kindsStruct{}},
		{Shade: ptr("BLUE")},
		{When: ptr(time.Unix(1, 0))},
		{When: ptr(time.Time{})},
		{Number: ptr(int64(1)), Text: ptr("a")},
		{Colors: []string{"NOPE"}},
		{Shade: ptr("NOPE")},
		{Signed: map[int]string{1: "NOPE"}},
		{Signed: map[int]string{math.MaxInt64: "RED"}},
		{Nested: map[uint32]kindsStruct{1: {}, 2: {Name: ptr("n"), Count: 4}, 3: {Count: 5}}},
		{Children: []kindsStruct{{}, {Child: &kindsStruct{}}}},
		{Stamp: time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Stamps: []time.Time{{}, time.Unix(1, 0)}, Sizes: []int64{0, 1}, Maybe: ptr(false), Wait: -time.Nanosecond},
		{Ratio: math.MaxFloat64},
		{Doubles: []float32{float32(math.NaN()), negZero}, Blobs: [][]byte{nil, {}, {2}}},
		{Point: &kindsPoint{}},
		{IDs: map[uint64]int{1: -1}},
	} {
		checkEncode(t, encode, k)
	}

	// Bytes by pointer: set to nothing, the field is set, and empty, with explicit presence only.
	type bytesField struct{ Data *[]byte }
	for _, md := range []protoreflect.MessageDescriptor{descriptorOf(&gentest.Kinds{}), descriptorOf(&gentest.Hybrid{})} {
		pair := bindingPair[bytesField](t, md, false)
		for _, v := range []bytesField{{}, {Data: new([]byte)}, {Data: ptr([]byte{})}, {Data: ptr([]byte{1})}} {
			checkEncode(t, pair, v)
		}
	}
}

func TestDirectBindingAPILevels(t *testing.T) {
	hybrid := bindingPair[levelsStruct](t, descriptorOf(&gentest.Hybrid{}), true)
	for _, msg := range []*gentest.Hybrid{
		{Name: "a", N: proto.Int32(0), Tags: []string{"x"}, Counts: map[string]int32{"c": 1}, Data: []byte{},
			Choice: &gentest.Hybrid_Child{Child: &gentest.Hybrid{Name: "c"}}, Sub: &gentest.Hybrid{N: proto.Int32(2)}},
		{Choice: &gentest.Hybrid_Text{}},
		{},
	} {
		_, ok := checkDecode(t, hybrid, msg)
		mustAgree(t, ok, "the hybrid message")
	}
	opaque := bindingPair[levelsStruct](t, descriptorOf(&gentest.Opaque{}), true)
	_, ok := checkDecode(t, opaque, gentest.Opaque_builder{Name: "a", N: proto.Int32(0), Tags: []string{"x"},
		Text: proto.String(""), Sub: gentest.Opaque_builder{Data: []byte{1}}.Build()}.Build())
	mustAgree(t, ok, "the opaque message")

	values := []levelsStruct{
		{Name: "a", N: ptr(int32(0)), Tags: []string{"x"}, Counts: map[string]int{"c": 1}, Child: &levelsStruct{},
			Sub: &levelsStruct{Text: ptr("t")}, Data: []byte{}},
		{},
		{Counts: map[string]int{"c": math.MaxInt64}},
	}
	for _, md := range []protoreflect.MessageDescriptor{descriptorOf(&gentest.Hybrid{}), descriptorOf(&gentest.Opaque{})} {
		encode := bindingPair[levelsStruct](t, md, false)
		for i, v := range values {
			if ok := checkEncode(t, encode, v); i < 2 {
				mustAgree(t, ok, string(md.FullName()))
			}
		}
	}

	op := gentest.Opaque_builder{Name: "o", Child: gentest.Opaque_builder{}.Build()}.Build()
	mixed := bindingPair[mixedStruct](t, descriptorOf(&gentest.Mixed{}), true)
	for _, msg := range []*gentest.Mixed{
		{},
		{Label: "l", One: op, Many: []*gentest.Opaque{op, nil}, ByKey: map[string]*gentest.Opaque{"a": op, "b": nil},
			Pick: &gentest.Mixed_Either{Either: op}},
		{Pick: &gentest.Mixed_Either{}},
		{Pick: &gentest.Mixed_Word{Word: "w"}},
	} {
		_, ok := checkDecode(t, mixed, msg)
		mustAgree(t, ok, "the mixed message")
	}
	encodeMixed := bindingPair[mixedStruct](t, descriptorOf(&gentest.Mixed{}), false)
	for i, v := range []mixedStruct{
		{},
		{Label: "l", One: &levelsStruct{Name: "o"}, Many: []levelsStruct{{}, {Tags: []string{"t"}}},
			ByKey: map[string]*levelsStruct{"a": {N: ptr(int32(1))}}, Either: &levelsStruct{}},
		{ByKey: map[string]*levelsStruct{"a": nil}},
		{Either: &levelsStruct{}, Word: ptr("w")},
	} {
		if ok := checkEncode(t, encodeMixed, v); i < 2 {
			mustAgree(t, ok, "the mixed message")
		}
	}
}

// directCase converts the messages a fuzz input decodes to, both ways, then the results back.
type directCase[T any] struct {
	newMessage func() proto.Message
	decode     [2]binding[T]
	encode     [2]binding[T]
}

func newDirectCase[T any](t testing.TB, m proto.Message) directCase[T] {
	return directCase[T]{
		newMessage: func() proto.Message { return m.ProtoReflect().New().Interface() },
		decode:     bindingPair[T](t, descriptorOf(m), true),
		encode:     bindingPair[T](t, descriptorOf(m), false),
	}
}

func (c directCase[T]) check(t *testing.T, data []byte) {
	m := c.newMessage()
	if (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(data, m) != nil {
		return
	}
	if v, ok := checkDecode(t, c.decode, m); ok {
		checkEncode(t, c.encode, v)
	}
}

func FuzzDirectBinding(f *testing.F) {
	for _, m := range []proto.Message{
		genUser(f, fullUserJSON),
		kindsMessage(),
		&gentest.Kinds{Choice: &gentest.Kinds_Sub{Sub: kindsMessage()}},
		&gentest.Scalars{I32: 1, Fl: 1.5, Count: wrapperspb.Int64(2), Names: map[string]string{"u-1": "a"}},
		&gentest.Hybrid{Name: "a", N: proto.Int32(1), Choice: &gentest.Hybrid_Text{Text: "t"}},
	} {
		f.Add(must(proto.MarshalOptions{AllowPartial: true}.Marshal(m)))
	}
	cases := []interface{ check(*testing.T, []byte) }{
		newDirectCase[user](f, &gentest.User{}),
		newDirectCase[scalars](f, &gentest.Scalars{}),
		newDirectCase[kindsStruct](f, &gentest.Kinds{}),
		newDirectCase[levelsStruct](f, &gentest.Hybrid{}),
		newDirectCase[mixedStruct](f, &gentest.Mixed{}),
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, c := range cases {
			c.check(t, data)
		}
	})
}
