package grpc

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/redact"
	"github.com/efegreat/forgo/server/grpc/internal/gentest"

	"github.com/rs/zerolog"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// kindsProto is forgotest/kinds.proto, a proto2 schema with what users.proto lacks for the JSON of logged
// messages: map keys of every kind, groups, NullValue, Any, Empty, required fields and extensions. Its
// messages are registered as dynamicpb types.
const kindsProto = `
name: "forgotest/kinds.proto"
package: "forgotest"
syntax: "proto2"
dependency: ["google/protobuf/any.proto", "google/protobuf/empty.proto", "google/protobuf/duration.proto",
             "google/protobuf/wrappers.proto", "google/protobuf/struct.proto", "google/protobuf/timestamp.proto"]
enum_type {
  name: "Color"
  value { name: "RED" number: 0 }
  value { name: "GREEN" number: 1 }
}
message_type {
  name: "Kinds"
  field { name: "b" number: 1 label: LABEL_OPTIONAL type: TYPE_BOOL }
  field { name: "f" number: 2 label: LABEL_OPTIONAL type: TYPE_FLOAT }
  field { name: "d" number: 3 label: LABEL_OPTIONAL type: TYPE_DOUBLE }
  field { name: "i64" number: 4 label: LABEL_OPTIONAL type: TYPE_INT64 }
  field { name: "u32" number: 5 label: LABEL_OPTIONAL type: TYPE_UINT32 }
  field { name: "sf64" number: 6 label: LABEL_OPTIONAL type: TYPE_SFIXED64 }
  field { name: "data" number: 7 label: LABEL_OPTIONAL type: TYPE_BYTES }
  field { name: "s" number: 8 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "null" number: 9 label: LABEL_OPTIONAL type: TYPE_ENUM type_name: ".google.protobuf.NullValue" }
  field { name: "flags" number: 10 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds.FlagsEntry" }
  field { name: "ids" number: 11 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds.IdsEntry" }
  field { name: "signed" number: 12 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds.SignedEntry" }
  field { name: "nested" number: 13 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds.NestedEntry" }
  field { name: "named" number: 14 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds.NamedEntry" }
  field { name: "colors" number: 15 label: LABEL_REPEATED type: TYPE_ENUM type_name: ".forgotest.Color" }
  field { name: "child" number: 16 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Kinds" }
  field { name: "floats" number: 17 label: LABEL_REPEATED type: TYPE_FLOAT }
  field { name: "point" number: 18 label: LABEL_OPTIONAL type: TYPE_GROUP type_name: ".forgotest.Kinds.Point" }
  field { name: "any" number: 20 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Any" }
  field { name: "empty" number: 21 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Empty" }
  field { name: "dur" number: 22 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Duration" }
  field { name: "bv" number: 23 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.BoolValue" }
  field { name: "bytesv" number: 24 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.BytesValue" }
  field { name: "fv" number: 25 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.FloatValue" }
  field { name: "lv" number: 26 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.ListValue" }
  field { name: "stamps" number: 27 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" }
  field { name: "u64v" number: 28 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.UInt64Value" }
  field { name: "children" number: 29 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Kinds" }
  field { name: "e" number: 30 label: LABEL_OPTIONAL type: TYPE_ENUM type_name: ".forgotest.Color" }
  field { name: "req" number: 31 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Required" }
  nested_type {
    name: "FlagsEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_BOOL }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
    options { map_entry: true }
  }
  nested_type {
    name: "IdsEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_UINT64 }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_INT32 }
    options { map_entry: true }
  }
  nested_type {
    name: "SignedEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_SINT32 }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
    options { map_entry: true }
  }
  nested_type {
    name: "NestedEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_FIXED32 }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Kinds" }
    options { map_entry: true }
  }
  nested_type {
    name: "NamedEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Kinds" }
    options { map_entry: true }
  }
  nested_type {
    name: "Point"
    field { name: "x" number: 19 label: LABEL_OPTIONAL type: TYPE_INT32 }
  }
}
message_type {
  name: "Required"
  field { name: "n" number: 1 label: LABEL_REQUIRED type: TYPE_INT32 }
}
message_type {
  name: "Strict"
  field { name: "id" number: 1 label: LABEL_REQUIRED type: TYPE_STRING }
  field { name: "note" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
  extension_range { start: 100 end: 201 }
}
extension { name: "tag" number: 100 label: LABEL_OPTIONAL type: TYPE_STRING extendee: ".forgotest.Strict" }
`

var kindsFile = func() protoreflect.FileDescriptor {
	_ = usersFile // registers the dependencies users.proto shares
	var fdp descriptorpb.FileDescriptorProto
	if err := prototext.Unmarshal([]byte(kindsProto), &fdp); err != nil {
		panic(err)
	}
	fd, err := protodesc.NewFile(&fdp, protoregistry.GlobalFiles)
	if err != nil {
		panic(err)
	}
	if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
		panic(err)
	}
	for i := range fd.Messages().Len() {
		if err := protoregistry.GlobalTypes.RegisterMessage(dynamicpb.NewMessageType(fd.Messages().Get(i))); err != nil {
			panic(err)
		}
	}
	if err := protoregistry.GlobalTypes.RegisterExtension(dynamicpb.NewExtensionType(fd.Extensions().Get(0))); err != nil {
		panic(err)
	}
	return fd
}()

// kinds returns a forgotest.Kinds from its protojson, which may leave required fields unset.
func kinds(t testing.TB, js string) *dynamicpb.Message {
	t.Helper()
	m := dynamicpb.NewMessage(kindsFile.Messages().ByName("Kinds"))
	if err := (protojson.UnmarshalOptions{AllowPartial: true}).Unmarshal([]byte(js), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// set sets the field name of m to v, and returns m.
func set(m *dynamicpb.Message, name string, v protoreflect.Value) *dynamicpb.Message {
	m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(name)), v)
	return m
}

// chain returns n Users, each the manager of the one before, the last with extra set: a message whose JSON
// nests n objects deep, and deeper inside extra.
func chain(t testing.TB, n int, extra string) proto.Message {
	return chainInto(t, dynamicpb.NewMessage(message("User")), n, extra)
}

// genChain is chain in a generated forgo.gentest.User.
func genChain(t testing.TB, n int, extra string) proto.Message {
	return chainInto(t, new(gentest.User), n, extra)
}

func chainInto(t testing.TB, m proto.Message, n int, extra string) proto.Message {
	js := strings.Repeat(`{"manager":`, n-1) + `{"name":"x"` + extra + "}" + strings.Repeat("}", n-1)
	if err := (protojson.UnmarshalOptions{RecursionLimit: 4 * n}).Unmarshal([]byte(js), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// planOf returns the messageLog New plans for a method of m's message, nil when m is logged through
// protoreflect.
func planOf(m proto.Message) *messageLog {
	return planMessageLog(m.ProtoReflect().Descriptor(), map[protoreflect.FullName]*messageLog{})
}

// checkMessageJSON checks that appendMessageJSON writes what LogJSON makes of protojson's encoding of m:
// the same JSON, an error where protojson fails, and errTooDeep where LogJSON does not follow the JSON. A
// message with a messageLog is written the same way through it and through protoreflect.
func checkMessageJSON(t testing.TB, m proto.Message, fields redact.Fields) {
	t.Helper()
	got, err := appendMessageJSON(nil, m, nil, fields)
	if plan := planOf(m); plan != nil {
		direct, derr := appendMessageJSON(nil, m, plan, fields)
		if fmt.Sprint(derr) != fmt.Sprint(err) || err == nil && !bytes.Equal(direct, got) {
			t.Errorf("read directly, appendMessageJSON =\n%.300s, %v\nthrough protoreflect\n%.300s, %v", direct, derr, got, err)
		}
	}
	raw, perr := messageJSON.Marshal(m)
	if perr != nil {
		if err == nil {
			t.Errorf("appendMessageJSON = %s, but protojson fails: %v", got, perr)
		}
		return
	}
	want, valid := fields.AppendJSON(nil, raw)
	switch {
	case !valid && err != errTooDeep:
		t.Errorf("appendMessageJSON = %.200s, %v; LogJSON does not follow it", got, err)
	case valid && err != nil:
		t.Errorf("appendMessageJSON fails: %v; want %.200s", err, want)
	case valid && !bytes.Equal(got, want):
		t.Errorf("appendMessageJSON =\n%s\nwant\n%s", got, want)
	}
}

func TestMessageJSON(t *testing.T) {
	invalid := string([]byte{'a', 0xff, 'b'})
	stamp := func(secs int64, nanos int32) protoreflect.Value {
		return protoreflect.ValueOfMessage((&timestamppb.Timestamp{Seconds: secs, Nanos: nanos}).ProtoReflect())
	}
	duration := func(secs int64, nanos int32) *dynamicpb.Message {
		m := kinds(t, `{}`)
		d := m.Mutable(m.Descriptor().Fields().ByName("dur")).Message()
		d.Set(d.Descriptor().Fields().ByNumber(1), protoreflect.ValueOfInt64(secs))
		d.Set(d.Descriptor().Fields().ByNumber(2), protoreflect.ValueOfInt32(nanos))
		return m
	}
	stamps := func(values ...protoreflect.Value) *dynamicpb.Message {
		m := kinds(t, `{}`)
		l := m.Mutable(m.Descriptor().Fields().ByName("stamps")).List()
		for _, v := range values {
			l.Append(v)
		}
		return m
	}
	anyOf := func(m proto.Message) *dynamicpb.Message {
		a, err := anypb.New(m)
		if err != nil {
			t.Fatal(err)
		}
		return set(kinds(t, `{}`), "any", protoreflect.ValueOfMessage(a.ProtoReflect()))
	}
	strict := dynamicpb.NewMessage(kindsFile.Messages().ByName("Strict"))
	strict.Set(strict.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("s-1"))
	tag, err := protoregistry.GlobalTypes.FindExtensionByName("forgotest.tag")
	if err != nil {
		t.Fatal(err)
	}
	strict.Set(tag.TypeDescriptor(), protoreflect.ValueOfString("ext"))
	// Set past protojson's checks on input.
	invalidList := newMessage(t, "ImportReport", `{"ids": ["a"]}`).(*dynamicpb.Message)
	invalidList.Mutable(message("ImportReport").Fields().ByName("ids")).List().Append(protoreflect.ValueOfString(invalid))
	invalidKey := kinds(t, `{}`)
	invalidKey.Mutable(invalidKey.Descriptor().Fields().ByName("named")).Map().
		Set(protoreflect.ValueOfString(invalid).MapKey(), protoreflect.ValueOfMessage(kinds(t, `{}`)))

	cases := []struct {
		name string
		m    proto.Message
	}{
		{"generated", &testpb.SimpleRequest{ResponseSize: 4, FillUsername: true, Payload: &testpb.Payload{Body: []byte("payload")}}},
		{"generated empty", &testpb.SimpleResponse{}},
		{"generated nil", (*testpb.SimpleRequest)(nil)},
		{"well-known type", &emptypb.Empty{}},
		{"top-level wrapper", wrapperspb.String("ada")},
		{"top-level struct", must(structpb.NewStruct(map[string]any{"name": "ada", "b": map[string]any{"name": 1}}))},
		{"user", newMessage(t, "User", fullUserJSON)},
		{"scalars", newMessage(t, "Scalars", `{"i32": -1, "i64": "-9223372036854775808", "u32": 4294967295,
			"u64": "18446744073709551615", "s32": -2147483648, "s64": "9223372036854775807", "f32": 1, "f64": "2",
			"sf32": -3, "sf64": "-4", "fl": 3.4028235e38, "db": 5e-324, "list": [1, "a", null, true, {"k": []}],
			"count": "-5", "stamp": "0001-01-01T00:00:00Z", "names": {"b": "1", "a": "2", "": "3"}}`)},
		{"kinds", kinds(t, `{"b": true, "f": 1e-7, "d": 1e21, "i64": "7", "u32": 8, "sf64": "-9", "data": "AAEC/w==",
			"s": "q\"b\\c\u0000\u001f\b\f\n\r\t\u007f �😀", "null": null,
			"flags": {"true": "t", "false": "f"}, "ids": {"18446744073709551615": 1, "2": 2, "10": 3},
			"signed": {"-1": "a", "-10": "b", "3": "c"}, "nested": {"4294967295": {"b": true}, "0": {}},
			"named": {"z": {"s": "x"}, "a\"b": {}, "name": {"i64": "1"}}, "colors": ["GREEN", "RED"],
			"child": {"child": {"s": "deep"}}, "floats": [0, -0, 1.5, "NaN", "Infinity", "-Infinity", 1e-7, 3e38],
			"Point": {"x": 3}, "empty": {}, "dur": "-1.000500s", "bv": false, "bytesv": "", "fv": "NaN",
			"lv": [{"a": 1}], "stamps": ["2026-01-02T03:04:05.123456789Z", "2026-01-02T03:04:05.120Z",
			"9999-12-31T23:59:59.999999999Z"], "u64v": "18446744073709551615", "children": [{}, {"e": "GREEN"}],
			"e": "RED"}`)},
		{"float formats", kinds(t, `{"f": 1e21, "d": 1e-6, "floats": [1e20, 1e21, 9.999999e-7, 123456789, -1e-300]}`)},
		{"unknown enum numbers", set(set(kinds(t, `{"colors": ["GREEN"]}`), "e", protoreflect.ValueOfEnum(42)),
			"null", protoreflect.ValueOfEnum(7))},
		{"durations", kinds(t, `{"dur": "315576000000.999999999s"}`)},
		{"negative duration", kinds(t, `{"dur": "-0.5s"}`)},
		{"duration out of range", duration(315576000001, 0)},
		{"duration nanos out of range", duration(0, 1e9)},
		{"duration signs differ", duration(1, -1)},
		{"duration of nanos", duration(0, -1)},
		{"timestamp out of range", stamps(stamp(253402300800, 0))},
		{"timestamp before its range", stamps(stamp(-62135596801, 0))},
		{"timestamp nanos out of range", stamps(stamp(0, -1))},
		{"timestamp at nanos limit", stamps(stamp(0, 999999999))},
		{"any", anyOf(wrapperspb.String("packed"))},
		{"any of a message", anyOf(&testpb.Payload{Body: []byte("x")})},
		{"invalid UTF-8", set(kinds(t, `{}`), "s", protoreflect.ValueOfString(invalid))},
		{"invalid UTF-8 in a list", invalidList},
		{"invalid UTF-8 in a map key", invalidKey},
		{"required and extension", strict},
		{"missing required field", dynamicpb.NewMessage(kindsFile.Messages().ByName("Strict"))},
		{"missing nested required field", kinds(t, `{"req": {}}`)},
		{"required field", kinds(t, `{"req": {"n": 1}}`)},
		{"deepest", chain(t, redact.MaxJSONDepth, "")},
		{"too deep", chain(t, redact.MaxJSONDepth+1, "")},
		{"too deep in a list", chain(t, redact.MaxJSONDepth-1, `,"previous":[{}]`)},
		{"too deep list", chain(t, redact.MaxJSONDepth, `,"tags":["a"]`)},
		{"too deep map", chain(t, redact.MaxJSONDepth, `,"labels":{"a":"b"}`)},
		{"too deep in a well-known type", chain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":{}}`)},
		{"deepest well-known type", chain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":1}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, fields := range []redact.Fields{
				{},
				redact.NewFields("name", "S", "body", "plan", "zip_code", "z", `a"b`, "true", "18446744073709551615", "-1", "x"),
				redact.NewFields("attributes", "named", "stamps", "dur", "any", "point", "tag", "manager"),
			} {
				checkMessageJSON(t, tc.m, fields)
			}
		})
	}
}

// Generated messages are logged directly, through the messageLog of their type, as through protoreflect and
// as LogJSON writes protojson's encoding.
func TestMessageJSONGenerated(t *testing.T) {
	invalid := string([]byte{'a', 0xff, 'b'})
	nan, negZero := math.NaN(), math.Copysign(0, -1)
	// withID sets the required field of a forgo.gentest.Kinds, which would fail the message otherwise.
	withID := func(k *gentest.Kinds) *gentest.Kinds {
		k.Id = proto.String("k")
		return k
	}
	negZero32 := float32(negZero)

	cases := []struct {
		name string
		m    proto.Message
	}{
		{"user", genUser(t, fullUserJSON)},
		{"typical user", genUser(t, typicalUserJSON)},
		{"nil", (*gentest.User)(nil)},
		{"empty", &gentest.User{}},
		{"special values", &gentest.User{Score: nan, Avatar: []byte{}, Nick: proto.String(""), Contact: &gentest.User_Email{}}},
		{"negative zero", &gentest.User{Score: negZero, Contact: &gentest.User_Phone{Phone: "1"}}},
		{"nil oneof wrapper", &gentest.User{Contact: (*gentest.User_Email)(nil)}},
		{"nil elements", &gentest.User{Previous: []*gentest.Address{nil, {Street: "a"}}, Counts: map[int32]*gentest.Address{3: nil, 4: {}}}},
		{"recursive", &gentest.User{Manager: &gentest.User{Manager: &gentest.User{Name: "c"}}}},
		{"unknown enum number", &gentest.User{Status: 9, Level: -1}},
		{"invalid UTF-8", &gentest.User{Name: invalid}},
		{"invalid UTF-8 in a list", &gentest.User{Tags: []string{"a", invalid}}},
		{"invalid UTF-8 in a nested message", &gentest.User{Address: &gentest.Address{Street: invalid}}},
		{"invalid UTF-8 in a map key", &gentest.User{Labels: map[string]string{invalid: "x"}}},
		{"kinds", kindsMessage()},
		{"missing required field", &gentest.Kinds{Name: proto.String("n")}},
		{"missing nested required field", withID(&gentest.Kinds{Child: &gentest.Kinds{}})},
		{"explicit zero values", withID(&gentest.Kinds{Name: proto.String(""), Count: proto.Int32(0), Big: proto.Uint64(0),
			Ratio: &negZero32, On: proto.Bool(false), Data: []byte{}, Color: gentest.Color_RED.Enum()})},
		{"oneof text", withID(&gentest.Kinds{Choice: &gentest.Kinds_Text{}})},
		{"oneof blob", withID(&gentest.Kinds{Choice: &gentest.Kinds_Blob{}})},
		{"oneof empty blob", withID(&gentest.Kinds{Choice: &gentest.Kinds_Blob{Blob: []byte{}}})},
		{"oneof nil message", withID(&gentest.Kinds{Choice: &gentest.Kinds_Sub{}})},
		{"oneof message", withID(&gentest.Kinds{Choice: &gentest.Kinds_Sub{Sub: withID(&gentest.Kinds{Name: proto.String("s")})}})},
		{"oneof enum", withID(&gentest.Kinds{Choice: &gentest.Kinds_Shade{Shade: gentest.Color_BLUE}})},
		{"oneof unknown enum number", withID(&gentest.Kinds{Choice: &gentest.Kinds_Shade{Shade: 9}})},
		{"oneof nil timestamp", withID(&gentest.Kinds{Choice: &gentest.Kinds_When{}})},
		{"oneof timestamp", withID(&gentest.Kinds{Choice: &gentest.Kinds_When{When: timestamppb.New(time.Unix(9, 0))}})},
		{"oneof nil wrapper", withID(&gentest.Kinds{Choice: (*gentest.Kinds_Text)(nil)})},
		{"timestamp before its range", withID(&gentest.Kinds{Stamp: &timestamppb.Timestamp{Seconds: minTimestampSeconds - 1}})},
		{"timestamp nanos out of range", withID(&gentest.Kinds{Stamps: []*timestamppb.Timestamp{{Nanos: 1e9}}})},
		{"duration signs differ", withID(&gentest.Kinds{Wait: &durationpb.Duration{Seconds: -1, Nanos: 1}})},
		{"duration out of range", withID(&gentest.Kinds{Wait: &durationpb.Duration{Seconds: maxDurationSeconds + 1}})},
		{"float formats", withID(&gentest.Kinds{Ratio: proto.Float32(1e21), Doubles: []float64{1e20, 1e21, 9.999999e-7, -1e-300, nan}})},
		{"scalars", &gentest.Scalars{I32: -1, I64: math.MinInt64, U32: math.MaxUint32, U64: math.MaxUint64, S32: math.MinInt32,
			S64: math.MaxInt64, F32: 1, F64: 2, Sf32: -3, Sf64: -4, Fl: math.MaxFloat32, Db: 5e-324,
			Count: wrapperspb.Int64(-5), Stamp: &timestamppb.Timestamp{Seconds: minTimestampSeconds},
			Names: map[string]string{"b": "1", "a": "2", "": "3"}}},
		{"hybrid", &gentest.Hybrid{Name: "h", N: proto.Int32(0), Tags: []string{"x"}, Counts: map[string]int32{"a": 1},
			Choice: &gentest.Hybrid_Child{Child: &gentest.Hybrid{Name: "c"}}, Sub: &gentest.Hybrid{Data: []byte{1}}}},
		{"opaque", gentest.Opaque_builder{Name: "o", N: proto.Int32(1), Tags: []string{"t"}, Text: proto.String("x")}.Build()},
		{"open holding opaque", &gentest.Mixed{Label: "m", One: gentest.Opaque_builder{Name: "one"}.Build(),
			Many:  []*gentest.Opaque{gentest.Opaque_builder{Sub: gentest.Opaque_builder{}.Build()}.Build(), nil},
			ByKey: map[string]*gentest.Opaque{"k": gentest.Opaque_builder{Data: []byte{2}}.Build()},
			Pick:  &gentest.Mixed_Either{Either: gentest.Opaque_builder{Name: "e"}.Build()}}},
		{"deepest", genChain(t, redact.MaxJSONDepth, "")},
		{"too deep", genChain(t, redact.MaxJSONDepth+1, "")},
		{"too deep in a list", genChain(t, redact.MaxJSONDepth-1, `,"previous":[{}]`)},
		{"too deep list", genChain(t, redact.MaxJSONDepth, `,"tags":["a"]`)},
		{"too deep map", genChain(t, redact.MaxJSONDepth, `,"labels":{"a":"b"}`)},
		{"too deep in a well-known type", genChain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":{}}`)},
		{"deepest well-known type", genChain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":1}`)},
		{"deepest timestamp", genChain(t, redact.MaxJSONDepth, `,"createdAt":"2026-01-02T03:04:05Z"`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, fields := range []redact.Fields{
				{},
				redact.NewFields("name", "Point", "street", "plan", "zip_code", "a", "k", "3", "true", "18446744073709551615", "-1"),
				redact.NewFields("attributes", "nested", "stamps", "wait", "manager", "children", "previous", "counts", "one", "many"),
			} {
				checkMessageJSON(t, tc.m, fields)
			}
		})
	}
}

// New plans a messageLog for the generated messages of the open and hybrid APIs, and their messages, but not
// for the others, which are logged through protoreflect.
func TestMessageLogPlans(t *testing.T) {
	for _, tc := range []struct {
		name    string
		md      protoreflect.MessageDescriptor
		planned bool
	}{
		{"open", descriptorOf(&gentest.User{}), true},
		{"proto2", descriptorOf(&gentest.Kinds{}), true},
		{"hybrid", descriptorOf(&gentest.Hybrid{}), true},
		{"open holding opaque", descriptorOf(&gentest.Mixed{}), true},
		{"empty", descriptorOf(&emptypb.Empty{}), true},
		{"opaque", descriptorOf(&gentest.Opaque{}), false},
		{"dynamic", message("User"), false},
		{"timestamp", descriptorOf(&timestamppb.Timestamp{}), false},
		{"wrapper", descriptorOf(&wrapperspb.StringValue{}), false},
		{"struct", descriptorOf(&structpb.Struct{}), false},
		{"any", descriptorOf(&anypb.Any{}), false},
		{"extension ranges", descriptorOf(&descriptorpb.FieldOptions{}), false},
		{"no descriptor", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if plan := planMessageLog(tc.md, map[protoreflect.FullName]*messageLog{}); (plan != nil) != tc.planned {
				t.Fatalf("planned %v, want %v", plan != nil, tc.planned)
			}
		})
	}

	plans := map[protoreflect.FullName]*messageLog{}
	user := planMessageLog(descriptorOf(&gentest.User{}), plans)
	fieldOf := func(p *messageLog, name string) *fieldLog {
		for i := range p.fields {
			if p.fields[i].name == name {
				return &p.fields[i]
			}
		}
		t.Fatalf("no field %s", name)
		return nil
	}
	if f := fieldOf(user, "manager"); f.message != user {
		t.Error("a recursive message has a plan of its own")
	}
	if f := fieldOf(user, "address"); f.message != plans["forgo.gentest.Address"] || f.message == nil {
		t.Error("a nested message is not planned once")
	}
	if f := fieldOf(user, "created_at"); f.message != nil || f.shape != shapeMessage {
		t.Error("a Timestamp is planned field by field")
	}
	mixed := planMessageLog(descriptorOf(&gentest.Mixed{}), plans)
	if f := fieldOf(mixed, "one"); f.message != nil {
		t.Error("a message of the opaque API is planned")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// logMessageProtoJSON is logMessage as it was before appendMessageJSON.
func logMessageProtoJSON(event *zerolog.Event, key string, m proto.Message, limit int, fields redact.Fields) {
	if proto.Size(m) > limit {
		event.Bool(logger.BodyCutLogKey, true)
		return
	}
	buf, err := messageJSON.MarshalAppend(nil, m)
	if err == nil {
		fields.LogJSON(event, key, buf[:min(len(buf), limit)], len(buf) > limit)
	}
}

// logMessage logs the lines it did through protojson, the messages LogJSON cannot follow included.
func TestLogMessageMatchesProtoJSON(t *testing.T) {
	line := func(log func(*zerolog.Event, string, proto.Message, int, redact.Fields), m proto.Message, fields redact.Fields) string {
		var buf bytes.Buffer
		lg := zerolog.New(&buf)
		e := lg.Info()
		log(e, "body", m, 1<<20, fields)
		e.Send()
		return buf.String()
	}
	planned := func(event *zerolog.Event, key string, m proto.Message, limit int, fields redact.Fields) {
		logMessage(event, key, m, planOf(m), limit, fields)
	}
	invalid := set(kinds(t, `{}`), "s", protoreflect.ValueOfString(string([]byte{0xff})))
	for _, m := range []proto.Message{
		newMessage(t, "User", fullUserJSON),
		invalid,
		chain(t, redact.MaxJSONDepth, ""),
		chain(t, redact.MaxJSONDepth+1, ""),
		chain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":{}}`),
		genUser(t, fullUserJSON),
		&gentest.User{Name: string([]byte{0xff})},
		genChain(t, redact.MaxJSONDepth, ""),
		genChain(t, redact.MaxJSONDepth+1, ""),
		genChain(t, redact.MaxJSONDepth-1, `,"attributes":{"a":{}}`),
	} {
		for _, fields := range []redact.Fields{{}, redact.NewFields("name", "street")} {
			if got, want := line(planned, m, fields), line(logMessageProtoJSON, m, fields); got != want {
				t.Errorf("logged\n%.300s\nwant\n%.300s", got, want)
			}
		}
	}
}

func FuzzMessageJSON(f *testing.F) {
	for _, m := range []proto.Message{
		newMessage(f, "User", fullUserJSON),
		kinds(f, `{"s": "x", "named": {"a": {"s": "y"}}, "nested": {"1": {"b": true}}, "floats": [1.5],
			"stamps": ["2026-01-02T03:04:05.1Z"], "dur": "1.5s", "any": {"@type": "type.googleapis.com/google.protobuf.StringValue", "value": "v"},
			"Point": {"x": 1}, "flags": {"true": "t"}, "signed": {"-3": "n"}}`),
		genUser(f, fullUserJSON),
		kindsMessage(),
		&gentest.Mixed{Label: "m", Many: []*gentest.Opaque{gentest.Opaque_builder{Name: "o"}.Build()}},
	} {
		b, err := (proto.MarshalOptions{AllowPartial: true}).Marshal(m)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	descriptors := []protoreflect.MessageDescriptor{kindsFile.Messages().ByName("Kinds"), message("User"), message("Scalars")}
	// The generated messages, which are logged directly too.
	generated := []func() proto.Message{
		func() proto.Message { return new(gentest.User) },
		func() proto.Message { return new(gentest.Kinds) },
		func() proto.Message { return new(gentest.Scalars) },
		func() proto.Message { return new(gentest.Hybrid) },
		func() proto.Message { return new(gentest.Mixed) },
	}
	fields := redact.NewFields("s", "name", "street", "key", "plan", "1", "true")
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, md := range descriptors {
			m := dynamicpb.NewMessage(md)
			if !unmarshalDynamic(data, m) {
				continue
			}
			checkMessageJSON(t, m, fields)
			checkMessageJSON(t, m, redact.Fields{})
		}
		for _, newMessage := range generated {
			m := newMessage()
			if (proto.UnmarshalOptions{AllowPartial: true}).Unmarshal(data, m) != nil {
				continue
			}
			checkMessageJSON(t, m, fields)
			checkMessageJSON(t, m, redact.Fields{})
		}
	})
}

// unmarshalDynamic reports whether data decodes into m. proto.Unmarshal panics on some map entries of a
// dynamic message, such as a key of the wrong wire type, which are skipped.
func unmarshalDynamic(data []byte, m *dynamicpb.Message) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return proto.Unmarshal(data, m) == nil
}

func TestLogMessageDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops pooled items at random under the race detector")
	}
	lg := zerolog.New(io.Discard)
	fields := redact.NewFields("body")
	req := &testpb.SimpleRequest{ResponseSize: 4, Payload: &testpb.Payload{Body: []byte("payload")}}
	resp := &testpb.SimpleResponse{Username: "ada", Payload: &testpb.Payload{Body: []byte("payload")}}
	user := genUser(t, typicalUserJSON)
	reqPlan, respPlan, userPlan := planOf(req), planOf(resp), planOf(user)
	allocs := testing.AllocsPerRun(100, func() {
		e := lg.Info()
		logMessage(e, logger.RequestBodyLogKey, req, reqPlan, RequestRecorderBodyLimit, fields)
		logMessage(e, logger.ResponseBodyLogKey, resp, respPlan, ResponseRecorderBodyLimit, fields)
		logMessage(e, logger.ResponseBodyLogKey, user, userPlan, ResponseRecorderBodyLimit, fields)
		e.Send()
	})
	if allocs != 0 {
		t.Fatalf("logMessage allocated %v times", allocs)
	}
}

// typicalUserJSON is a forgo.gentest.User as an API usually sends one: scalars, a nested message, a list of
// them, a list of strings, a oneof, a Timestamp and a Duration, without the maps and the well-known types
// that protojson encodes.
const typicalUserJSON = `{
	"id": "u-7", "name": "Ada", "age": 36, "score": 9.5, "active": true, "status": "STATUS_ACTIVE",
	"address": {"street": "Main", "zipCode": "1000"}, "previous": [{"street": "Old"}, {"street": "Older"}],
	"tags": ["a", "b"], "createdAt": "2026-10-15T12:00:00.500Z", "ttl": "90s", "email": "ada@example.com",
	"manager": {"id": "u-1", "name": "Grace"}
}`

func BenchmarkLogMessage(b *testing.B) {
	lg := zerolog.New(io.Discard)
	fields := redact.NewFields("body")
	req := &testpb.SimpleRequest{ResponseSize: 4, Payload: &testpb.Payload{Body: []byte("payload")}}
	user := newMessage(b, "User", fullUserJSON)
	stamp := timestamppb.New(time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC))
	// planned logs a message as the server does, with the messageLog New plans for it.
	planned := func(m proto.Message) func(*zerolog.Event, string, proto.Message, int, redact.Fields) {
		plan := planOf(m)
		return func(event *zerolog.Event, key string, m proto.Message, limit int, fields redact.Fields) {
			logMessage(event, key, m, plan, limit, fields)
		}
	}
	for _, c := range []struct {
		name string
		m    proto.Message
	}{
		{"generated", req},
		{"generated_user", genUser(b, typicalUserJSON)},
		{"generated_full_user", genUser(b, fullUserJSON)},
		{"dynamic_user", user},
		{"timestamp", stamp},
	} {
		for _, impl := range []struct {
			name string
			log  func(*zerolog.Event, string, proto.Message, int, redact.Fields)
		}{{"direct", planned(c.m)}, {"protojson", logMessageProtoJSON}} {
			b.Run(c.name+"/"+impl.name, func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					e := lg.Info()
					impl.log(e, "body", c.m, math.MaxInt32, fields)
					e.Send()
				}
			})
		}
	}
}
