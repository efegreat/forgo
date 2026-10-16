package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/efegreat/forgo"
	"github.com/efegreat/forgo/logger"
	"github.com/efegreat/forgo/operation"
	fhttp "github.com/efegreat/forgo/server/http"

	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	testpb "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

// usersProto is forgotest/users.proto, a schema with every kind of field. Its messages are registered as
// dynamicpb types, so the bindings convert messages that are not generated code too.
const usersProto = `
name: "forgotest/users.proto"
package: "forgotest"
syntax: "proto3"
dependency: ["google/protobuf/timestamp.proto", "google/protobuf/duration.proto", "google/protobuf/wrappers.proto",
             "google/protobuf/struct.proto", "google/protobuf/field_mask.proto"]
enum_type {
  name: "Status"
  value { name: "STATUS_UNSPECIFIED" number: 0 }
  value { name: "STATUS_ACTIVE" number: 1 }
  value { name: "STATUS_BLOCKED" number: 2 }
}
message_type {
  name: "Address"
  field { name: "street" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "zip_code" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
}
message_type {
  name: "User"
  field { name: "id" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "name" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "age" number: 3 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field { name: "score" number: 4 label: LABEL_OPTIONAL type: TYPE_DOUBLE }
  field { name: "active" number: 5 label: LABEL_OPTIONAL type: TYPE_BOOL }
  field { name: "avatar" number: 6 label: LABEL_OPTIONAL type: TYPE_BYTES }
  field { name: "status" number: 7 label: LABEL_OPTIONAL type: TYPE_ENUM type_name: ".forgotest.Status" }
  field { name: "level" number: 8 label: LABEL_OPTIONAL type: TYPE_ENUM type_name: ".forgotest.Status" }
  field { name: "address" number: 9 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Address" }
  field { name: "previous" number: 10 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Address" }
  field { name: "tags" number: 11 label: LABEL_REPEATED type: TYPE_STRING }
  field { name: "labels" number: 12 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.User.LabelsEntry" }
  field { name: "created_at" number: 13 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" }
  field { name: "ttl" number: 14 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Duration" }
  field { name: "nickname" number: 15 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.StringValue" }
  field { name: "attributes" number: 16 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Struct" }
  field { name: "extra" number: 17 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Value" }
  field { name: "mask" number: 18 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.FieldMask" }
  field { name: "email" number: 19 label: LABEL_OPTIONAL type: TYPE_STRING oneof_index: 0 }
  field { name: "phone" number: 20 label: LABEL_OPTIONAL type: TYPE_STRING oneof_index: 0 }
  field { name: "manager" number: 21 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.User" }
  field { name: "nick" number: 22 label: LABEL_OPTIONAL type: TYPE_STRING oneof_index: 1 proto3_optional: true }
  field { name: "counts" number: 23 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.User.CountsEntry" }
  field { name: "big" number: 24 label: LABEL_OPTIONAL type: TYPE_UINT64 }
  field { name: "created" number: 25 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" }
  oneof_decl { name: "contact" }
  oneof_decl { name: "_nick" }
  nested_type {
    name: "LabelsEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
    options { map_entry: true }
  }
  nested_type {
    name: "CountsEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".forgotest.Address" }
    options { map_entry: true }
  }
}
message_type {
  name: "GetUserRequest"
  field { name: "id" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
  field { name: "fields" number: 2 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.FieldMask" }
}
message_type {
  name: "ImportReport"
  field { name: "imported" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field { name: "ids" number: 2 label: LABEL_REPEATED type: TYPE_STRING }
}
message_type {
  name: "Scalars"
  field { name: "i32" number: 1 label: LABEL_OPTIONAL type: TYPE_INT32 }
  field { name: "i64" number: 2 label: LABEL_OPTIONAL type: TYPE_INT64 }
  field { name: "u32" number: 3 label: LABEL_OPTIONAL type: TYPE_UINT32 }
  field { name: "u64" number: 4 label: LABEL_OPTIONAL type: TYPE_UINT64 }
  field { name: "s32" number: 5 label: LABEL_OPTIONAL type: TYPE_SINT32 }
  field { name: "s64" number: 6 label: LABEL_OPTIONAL type: TYPE_SINT64 }
  field { name: "f32" number: 7 label: LABEL_OPTIONAL type: TYPE_FIXED32 }
  field { name: "f64" number: 8 label: LABEL_OPTIONAL type: TYPE_FIXED64 }
  field { name: "sf32" number: 9 label: LABEL_OPTIONAL type: TYPE_SFIXED32 }
  field { name: "sf64" number: 10 label: LABEL_OPTIONAL type: TYPE_SFIXED64 }
  field { name: "fl" number: 11 label: LABEL_OPTIONAL type: TYPE_FLOAT }
  field { name: "db" number: 12 label: LABEL_OPTIONAL type: TYPE_DOUBLE }
  field { name: "list" number: 13 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.ListValue" }
  field { name: "count" number: 14 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Int64Value" }
  field { name: "stamp" number: 15 label: LABEL_OPTIONAL type: TYPE_MESSAGE type_name: ".google.protobuf.Timestamp" }
  field { name: "names" number: 16 label: LABEL_REPEATED type: TYPE_MESSAGE type_name: ".forgotest.Scalars.NamesEntry" }
  nested_type {
    name: "NamesEntry"
    field { name: "key" number: 1 label: LABEL_OPTIONAL type: TYPE_STRING }
    field { name: "value" number: 2 label: LABEL_OPTIONAL type: TYPE_STRING }
    options { map_entry: true }
  }
}
service {
  name: "Users"
  method { name: "Get" input_type: ".forgotest.GetUserRequest" output_type: ".forgotest.User" }
  method { name: "Watch" input_type: ".forgotest.GetUserRequest" output_type: ".forgotest.User" server_streaming: true }
  method { name: "Import" input_type: ".forgotest.User" output_type: ".forgotest.ImportReport" client_streaming: true }
  method { name: "Sync" input_type: ".forgotest.User" output_type: ".forgotest.User" client_streaming: true server_streaming: true }
}
`

var usersFile = func() protoreflect.FileDescriptor {
	var fdp descriptorpb.FileDescriptorProto
	if err := prototext.Unmarshal([]byte(usersProto), &fdp); err != nil {
		panic(err)
	}
	fd, err := protodesc.NewFile(&fdp, protoregistry.GlobalFiles)
	if err != nil {
		panic(err)
	}
	if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
		panic(err)
	}
	var register func(protoreflect.MessageDescriptors)
	register = func(mds protoreflect.MessageDescriptors) {
		for i := range mds.Len() {
			if md := mds.Get(i); !md.IsMapEntry() {
				if err := protoregistry.GlobalTypes.RegisterMessage(dynamicpb.NewMessageType(md)); err != nil {
					panic(err)
				}
				register(md.Messages())
			}
		}
	}
	register(fd.Messages())
	return fd
}()

// usersDesc is the descriptor protoc-gen-go-grpc would generate for forgotest.Users.
var usersDesc = grpc.ServiceDesc{
	ServiceName: "forgotest.Users",
	Methods:     []grpc.MethodDesc{{MethodName: "Get"}},
	Streams: []grpc.StreamDesc{
		{StreamName: "Watch", ServerStreams: true},
		{StreamName: "Import", ClientStreams: true},
		{StreamName: "Sync", ClientStreams: true, ServerStreams: true},
	},
	Metadata: "forgotest/users.proto",
}

func message(name string) protoreflect.MessageDescriptor {
	return usersFile.Messages().ByName(protoreflect.Name(name))
}

// newMessage returns a message of forgotest, from its protojson.
func newMessage(t testing.TB, name, js string) proto.Message {
	t.Helper()
	m := dynamicpb.NewMessage(message(name))
	if err := protojson.Unmarshal([]byte(js), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// userID is an encoding.TextMarshaler, like a UUID type.
type userID struct{ n int }

func (u userID) MarshalText() ([]byte, error) {
	return []byte("u-" + strconv.Itoa(u.n)), nil
}

func (u *userID) UnmarshalText(b []byte) error {
	s, ok := strings.CutPrefix(string(b), "u-")
	if !ok {
		return errors.New("no u- prefix")
	}
	n, err := strconv.Atoi(s)
	u.n = n
	return err
}

type (
	userStatus string // an enum by name
	level      int32  // an enum by number
)

type address struct {
	Street  string `json:"street"`
	ZipCode string `json:"zip_code"`
}

// user is business code's user: it maps to forgotest.User.
type user struct {
	ID         userID            `json:"id"`
	Name       string            `json:"name" validate:"required"`
	Age        int8              `json:"age"`
	Score      float64           `json:"score"`
	Active     bool              `json:"active"`
	Avatar     []byte            `json:"avatar"`
	Status     userStatus        `json:"status"`
	Level      level             `json:"level"`
	Address    *address          `json:"address"`
	Previous   []address         `json:"previous"`
	Tags       []string          `json:"tags"`
	Labels     map[string]string `json:"labels"`
	CreatedAt  time.Time         `json:"created_at"`
	TTL        time.Duration     // by its Go name
	Nickname   *string           `json:"nickname"`
	Attributes map[string]any    `json:"attributes"`
	Extra      any               `json:"extra"`
	Mask       []string          `json:"mask"`
	Email      *string           `json:"email"`
	Phone      *string           `json:"phone"`
	Manager    *user             `json:"manager"`
	Nick       *string           `json:"nick"`
	Counts     map[int]*address  `json:"counts"`
	Big        uint64            `json:"big"`
	Created    *time.Time        `json:"created"`
	Password   string            `json:"-"` // hidden from JSON, so from gRPC too
}

func ptr[T any](v T) *T { return &v }

// fullUserJSON is a forgotest.User with every field set, as protojson writes it.
const fullUserJSON = `{
	"id": "u-7", "name": "Ada", "age": 36, "score": 9.5, "active": true, "avatar": "AQI=",
	"status": "STATUS_ACTIVE", "level": "STATUS_BLOCKED",
	"address": {"street": "Main", "zipCode": "1000"}, "previous": [{"street": "Old"}],
	"tags": ["a", "b"], "labels": {"team": "core"},
	"createdAt": "2026-10-15T12:00:00.500Z", "ttl": "90s", "nickname": "ace",
	"attributes": {"plan": "pro", "seats": 3}, "extra": [1, "x"], "mask": "name,address.zipCode",
	"email": "ada@example.com", "manager": {"id": "u-1", "name": "Grace"}, "nick": "",
	"counts": {"3": {"street": "x"}}, "big": "18446744073709551615", "created": "1970-01-01T00:00:00Z"
}`

// fullUser is fullUserJSON in business code.
func fullUser() user {
	return user{
		ID: userID{7}, Name: "Ada", Age: 36, Score: 9.5, Active: true, Avatar: []byte{1, 2},
		Status: "STATUS_ACTIVE", Level: 2,
		Address: &address{Street: "Main", ZipCode: "1000"}, Previous: []address{{Street: "Old"}},
		Tags: []string{"a", "b"}, Labels: map[string]string{"team": "core"},
		CreatedAt: time.Date(2026, 10, 15, 12, 0, 0, 5e8, time.UTC), TTL: 90 * time.Second, Nickname: ptr("ace"),
		Attributes: map[string]any{"plan": "pro", "seats": 3.0}, Extra: []any{1.0, "x"},
		Mask:  []string{"name", "address.zip_code"},
		Email: ptr("ada@example.com"), Manager: &user{ID: userID{1}, Name: "Grace"}, Nick: ptr(""),
		Counts: map[int]*address{3: {Street: "x"}}, Big: math.MaxUint64, Created: ptr(time.Unix(0, 0).UTC()),
	}
}

func mustBinding[T any](t testing.TB, md protoreflect.MessageDescriptor, decode bool) binding[T] {
	t.Helper()
	b, err := newBinding[T](md, decode)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestBindingToGo(t *testing.T) {
	b := mustBinding[user](t, message("User"), true)
	got, err := b.toGo(context.Background(), newMessage(t, "User", fullUserJSON))
	if err != nil {
		t.Fatal(err)
	}
	if want := fullUser(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}

	// Unset fields keep their zero value, pointers included.
	got, err = b.toGo(context.Background(), newMessage(t, "User", `{"name": "Ada"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, user{Name: "Ada"}) {
		t.Fatalf("got %+v", got)
	}

	// A pointer to a struct is allocated.
	bp := mustBinding[*user](t, message("User"), true)
	if p, err := bp.toGo(context.Background(), newMessage(t, "User", `{"name": "Ada"}`)); err != nil || p.Name != "Ada" {
		t.Fatalf("got %+v, %v", p, err)
	}
}

func TestBindingToProto(t *testing.T) {
	b := mustBinding[user](t, message("User"), false)
	u := fullUser()
	u.Password = "hunter2"
	m, err := b.toProto(u)
	if err != nil {
		t.Fatal(err)
	}
	got, err := protojson.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !jsonEqual(json.RawMessage(got), fullUserJSON) {
		t.Fatalf("got %s", got)
	}

	// Zero values are not set, and a pointer is converted like its value.
	m, err = mustBinding[*user](t, message("User"), false).toProto(&user{Name: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := protojson.Marshal(m); !jsonEqual(json.RawMessage(got), `{"name": "Ada"}`) {
		t.Fatalf("got %s", got)
	}
	if _, err := mustBinding[*user](t, message("User"), false).toProto(nil); !errors.Is(err, errNilResponse) {
		t.Fatalf("nil response: %v", err)
	}
}

// Generated messages convert like dynamic ones, and a field may hold the proto message itself.
func TestBindingGeneratedMessages(t *testing.T) {
	type payload struct {
		Type string // the PayloadType enum by name
		Body []byte
	}
	type echo struct {
		Payload  *payload
		Username string
		Status   *testpb.EchoStatus `grpc:"response_status"` // passed as it is
		_        struct{}           `grpc:"response_type"`
		_        struct{}           `grpc:"response_size"`
		_        struct{}           `grpc:"fill_username"`
		_        struct{}           `grpc:"fill_oauth_scope"`
		_        struct{}           `grpc:"response_compressed"`
		_        struct{}           `grpc:"expect_compressed"`
		_        struct{}           `grpc:"fill_server_id"`
		_        struct{}           `grpc:"fill_grpclb_route_type"`
		_        struct{}           `grpc:"orca_per_query_report"`
	}
	req := &testpb.SimpleRequest{
		Payload:        &testpb.Payload{Type: testpb.PayloadType_COMPRESSABLE, Body: []byte("hi")},
		ResponseStatus: &testpb.EchoStatus{Code: 5, Message: "five"},
		ResponseSize:   9, // ignored
	}
	in := (&testpb.SimpleRequest{}).ProtoReflect().Descriptor()
	got, err := mustBinding[echo](t, in, true).toGo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// COMPRESSABLE is 0, which proto3 does not set, so the string stays empty.
	if got.Payload == nil || got.Payload.Type != "" || string(got.Payload.Body) != "hi" || got.Status != req.ResponseStatus {
		t.Fatalf("got %+v", got)
	}

	type reply struct {
		Payload  payload
		Username string
	}
	out := (&testpb.SimpleResponse{}).ProtoReflect().Descriptor()
	m, err := mustBinding[reply](t, out, false).toProto(reply{Payload: payload{Body: []byte("x")}, Username: "ada"})
	if err != nil {
		t.Fatal(err)
	}
	resp, ok := m.(*testpb.SimpleResponse)
	if !ok || resp.GetUsername() != "ada" || string(resp.GetPayload().GetBody()) != "x" {
		t.Fatalf("got %T %v", m, m)
	}
}

func TestBindingRequestErrors(t *testing.T) {
	b := mustBinding[user](t, message("User"), true)
	timestamp := func(seconds int64) proto.Message {
		m := dynamicpb.NewMessage(message("User"))
		ts := timestamppb.New(time.Unix(seconds, 0))
		m.Set(message("User").Fields().ByName("created_at"), protoreflect.ValueOfMessage(ts.ProtoReflect()))
		return m
	}
	for _, tc := range []struct {
		name string
		msg  proto.Message
		path string
		err  string
	}{
		{"int out of range", newMessage(t, "User", `{"age": 300}`), "age", "300 is out of the range of int8"},
		{"unknown enum name", newMessage(t, "User", `{"status": 9}`), "status", "9 is not a value of forgotest.Status"},
		{"text", newMessage(t, "User", `{"id": "x-1"}`), "id", "no u- prefix"},
		{"nested", newMessage(t, "User", `{"manager": {"id": "7"}}`), "manager.id", "no u- prefix"},
		{"list", newMessage(t, "User", `{"previous": [{}, {"street": "x"}], "manager": {"age": -129}}`), "manager.age", "out of the range"},
		{"map", newMessage(t, "User", `{"counts": {"1": {}}, "manager": {"counts": {"2": {}}, "level": 3}}`), "", ""},
		{"timestamp", timestamp(math.MaxInt64), "created_at", "invalid timestamp"},
		{"duration", newMessage(t, "User", `{"ttl": "100000000000s"}`), "ttl", "invalid duration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := b.toGo(context.Background(), tc.msg)
			if tc.path == "" {
				if err != nil {
					t.Fatalf("got %v", err)
				}
				return
			}
			var fe *fieldError
			if !errors.As(err, &fe) || fe.Path() != tc.path || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %s: %s", err, tc.path, tc.err)
			}
			mapped, ok := MapBinding(err)
			if !ok || mapped.Code != "binding" || mapped.Message != "invalid field: "+tc.path {
				t.Fatalf("mapped %+v", mapped)
			}
		})
	}

	if _, err := b.toGo(context.Background(), newMessage(t, "Address", `{}`)); err == nil {
		t.Fatal("converted a message of another type")
	}
}

func TestBindingResponseErrors(t *testing.T) {
	b := mustBinding[user](t, message("User"), false)
	for _, tc := range []struct {
		name string
		u    user
		err  string
	}{
		{"oneof", user{Email: ptr("a"), Phone: ptr("b")}, "email and phone are members of one oneof"},
		{"enum name", user{Status: "STATUS_GONE"}, `status: "STATUS_GONE" is not a value of forgotest.Status`},
		{"nil element", user{Counts: map[int]*address{1: nil}}, "counts: nil element"},
		{"timestamp", user{CreatedAt: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}, "created_at: 10000-01-01"},
		{"nested", user{Manager: &user{Level: math.MaxInt32, Extra: make(chan int)}}, "manager.extra"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := b.toProto(tc.u)
			var oe interface{ Unwrap() error }
			if !errors.As(err, &oe) || !errors.Is(err, errInvalidResponse) || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %q", err, tc.err)
			}
		})
	}

	type wide struct {
		Age      int
		Big      int
		Previous []*address
	}
	bw := mustBinding[wide](t, message("User"), false)
	for _, w := range []wide{{Age: math.MaxInt32 + 1}, {Big: -1}, {Previous: []*address{nil}}} {
		if _, err := bw.toProto(w); !errors.Is(err, errInvalidResponse) {
			t.Fatalf("%+v: got %v", w, err)
		}
	}
	if m, err := bw.toProto(wide{Age: math.MaxInt32, Big: math.MaxInt}); err != nil || !strings.Contains(prototext.Format(m), "2147483647") {
		t.Fatalf("got %v, %v", m, err)
	}
}

// New fails on a type that does not fit its message, naming the field.
func TestBindingTypeErrors(t *testing.T) {
	type (
		missing struct {
			Street string
		}
		ignored struct {
			Street string
			_      struct{} `grpc:"zip_code"`
		}
		extra struct {
			Street, ZipCode, Country string
		}
		ambiguous struct {
			Street string
			Road   string `json:"street"`
		}
		twice struct {
			Street string `json:"street" query:"zip_code"`
		}
		wrongType struct {
			Street  int
			ZipCode string
		}
		unknownTag struct {
			Street  string `grpc:"road"`
			ZipCode string
		}
		inner   struct{ Street, ZipCode string }
		pointer struct{ *inner }
		value   struct {
			inner
		}
		oneofValue struct {
			Email string
		}
		listNoSlice struct{ Tags string }
		mapNoMap    struct{ Labels []string }
		wkt         struct{ CreatedAt string }
		header      struct {
			Street, ZipCode string
			Trace           chan int `header:"x-trace"`
		}
	)
	address, user := message("Address"), message("User")
	for _, tc := range []struct {
		name   string
		new    func() error
		decode bool
		err    string
	}{
		{"missing request field", func() error { _, err := newBinding[missing](address, true); return err },
			true, `grpc.missing has no field for forgotest.Address.zip_code; add one, or ignore it with a blank field tagged grpc:"zip_code"`},
		{"ignored request field", func() error { _, err := newBinding[ignored](address, true); return err }, true, ""},
		{"missing response field", func() error { _, err := newBinding[missing](address, false); return err }, false, ""},
		{"extra response field", func() error { _, err := newBinding[extra](address, false); return err },
			false, `grpc.extra.Country has no field in forgotest.Address; tag it grpc:"-" to leave it out`},
		{"extra request field", func() error { _, err := newBinding[extra](address, true); return err }, true, ""},
		{"ambiguous", func() error { _, err := newBinding[ambiguous](address, false); return err },
			false, "both Street and Road map to forgotest.Address.street"},
		{"twice", func() error { _, err := newBinding[twice](address, false); return err },
			false, "grpc.twice.Street maps to more than one field of forgotest.Address"},
		{"wrong type", func() error { _, err := newBinding[wrongType](address, false); return err },
			false, "grpc.wrongType.Street: forgotest.Address.street: cannot map string to int"},
		{"unknown grpc tag", func() error { _, err := newBinding[unknownTag](address, true); return err },
			true, `grpc.unknownTag.Street: forgotest.Address has no field "road"`},
		{"embedded pointer", func() error { _, err := newBinding[pointer](address, false); return err },
			false, "grpc.pointer.Street is promoted from an embedded pointer"},
		{"embedded value", func() error { _, err := newBinding[value](address, false); return err }, false, ""},
		{"oneof value", func() error { _, err := newBinding[oneofValue](user, false); return err },
			false, "forgotest.User.email is a member of oneof contact, so it needs a pointer"},
		{"list", func() error { _, err := newBinding[listNoSlice](user, false); return err },
			false, "a repeated field needs a slice, not string"},
		{"map", func() error { _, err := newBinding[mapNoMap](user, false); return err },
			false, "a map field needs a map, not []string"},
		{"well-known type", func() error { _, err := newBinding[wkt](user, false); return err },
			false, "cannot map message google.protobuf.Timestamp to string"},
		{"header", func() error { _, err := newBinding[header](address, true); return err },
			true, "grpc.header.Trace: metadata x-trace: cannot set chan int from metadata"},
		{"not a struct", func() error { _, err := newBinding[[]string](address, true); return err },
			true, "[]string is neither a pointer to a proto message nor a struct"},
		{"proto by value", func() error { _, err := newBinding[testpb.Empty](nil, true); return err },
			true, "grpc_testing.Empty is a proto message, which is used through a pointer: *grpc_testing.Empty"},
		{"unregistered", func() error { _, err := newBinding[inner](nil, true); return err },
			true, "the service's descriptor is not in the proto registry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.new()
			if tc.err == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("got %v, want %q", err, tc.err)
			}
		})
	}
}

// getUser is bound from the path and a header over HTTP, and from forgotest.GetUserRequest and the call's
// metadata over gRPC.
type getUser struct {
	ID     userID   `uri:"id" validate:"required"`
	Fields []string `query:"fields"`
	Tenant string   `header:"X-Tenant" validate:"required"`
	Trace  []string `header:"x-trace"`
	Limit  *int     `header:"x-limit"`
}

type importReport struct {
	Imported int      `json:"imported"`
	IDs      []userID `json:"ids"`
}

// userService is business code, without a generated type.
type userService struct{}

func newUserService() *userService { return &userService{} }

func (s *userService) Get(_ context.Context, req getUser) (user, error) {
	if req.ID.n == 404 {
		return user{}, errNotFound
	}
	return user{ID: req.ID, Name: "Ada", Tags: req.Fields, Labels: map[string]string{"tenant": req.Tenant}}, nil
}

var errNotFound = errors.New("not found")

func (s *userService) Watch(_ context.Context, req getUser, send func(*user) error) error {
	for i := range 3 {
		if err := send(&user{ID: userID{req.ID.n + i}, Name: "Ada"}); err != nil {
			return err
		}
	}
	return nil
}

func (s *userService) Import(_ context.Context, users iter.Seq2[user, error]) (importReport, error) {
	var report importReport
	for u, err := range users {
		if err != nil {
			return importReport{}, err
		}
		report.Imported++
		report.IDs = append(report.IDs, u.ID)
	}
	return report, nil
}

func (s *userService) Sync(_ context.Context, users iter.Seq2[*user, error], send func(*user) error) error {
	for u, err := range users {
		if err != nil {
			return err
		}
		u.Name = strings.ToUpper(u.Name)
		if err := send(u); err != nil {
			return err
		}
	}
	return nil
}

func usersConfig() ServiceConfig {
	return ServiceConfig{
		Desc: &usersDesc,
		Methods: []MethodConfig{
			{Method: "Get", Handler: HandlerFor((*userService).Get)},
			{Method: "Watch", Handler: ServerStreamFor((*userService).Watch)},
			{Method: "Import", Handler: ClientStreamFor((*userService).Import)},
			{Method: "Sync", Handler: BidiStreamFor((*userService).Sync)},
		},
	}
}

// userJSON returns the protojson of a message, decoded.
func userJSON(t testing.TB, m proto.Message) map[string]any {
	t.Helper()
	b, err := protojson.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Business methods without generated types serve every kind of method.
func TestBusinessService(t *testing.T) {
	h := newHarness(t, Config{
		SensitiveFields: []string{"x-limit"},
		Services:        []ServiceConfig{usersConfig()},
		ErrorMapper: operation.NewErrorMapper(MapBinding, MapValidation, func(err error) (operation.Error, bool) {
			if errors.Is(err, errNotFound) {
				return operation.Error{Code: "user_not_found", Status: operation.StatusNotFound, Message: "user not found"}, true
			}
			return operation.Error{}, false
		}),
	}, newUserService)
	ctx := metadata.AppendToOutgoingContext(testCtx(t), "x-tenant", "acme", "x-trace", "a", "x-trace", "b")

	t.Run("unary", func(t *testing.T) {
		resp := dynamicpb.NewMessage(message("User"))
		req := newMessage(t, "GetUserRequest", `{"id": "u-7", "fields": "name,tags"}`)
		if err := h.conn.Invoke(ctx, "/forgotest.Users/Get", req, resp); err != nil {
			t.Fatal(err)
		}
		got := userJSON(t, resp)
		if !jsonEqual(got, `{"id": "u-7", "name": "Ada", "tags": ["name", "tags"], "labels": {"tenant": "acme"}}`) {
			t.Fatalf("got %v", got)
		}
		started := h.line(t, "operation started")
		if !jsonEqual(started["req_body"], `{"id": "u-7", "fields": "name,tags"}`) {
			t.Fatalf("req_body %v", started["req_body"])
		}
		if done := h.line(t, "operation completed"); done["resp_body"] == nil {
			t.Fatalf("logged %v", done)
		}
	})

	t.Run("errors", func(t *testing.T) {
		resp := dynamicpb.NewMessage(message("User"))
		for _, tc := range []struct {
			name    string
			ctx     context.Context
			req     string
			code    codes.Code
			msg     string
			reason  string
			logged  string
			omitted string
		}{
			{"business", ctx, `{"id": "u-404"}`, codes.NotFound, "user not found", "user_not_found", "not found", ""},
			{"binding", ctx, `{"id": "7"}`, codes.InvalidArgument, "invalid field: id", "binding", "no u- prefix", ""},
			{"validation", testCtx(t), `{"id": "u-7"}`, codes.InvalidArgument, "field: X-Tenant, rule: required", "validation",
				"invalid grpc.getUser", ""},
			{"metadata", metadata.AppendToOutgoingContext(ctx, "x-limit", "12ab"), `{"id": "u-7"}`, codes.InvalidArgument,
				"invalid metadata: x-limit", "binding", "invalid syntax", "12ab"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := h.conn.Invoke(tc.ctx, "/forgotest.Users/Get", newMessage(t, "GetUserRequest", tc.req), resp)
				assertStatus(t, err, tc.code, tc.msg, tc.reason)
				logged, _ := h.line(t, "operation failed")["error"].(string)
				if !strings.Contains(logged, tc.logged) || (tc.omitted != "" && strings.Contains(logged, tc.omitted)) {
					t.Fatalf("logged %q", logged)
				}
			})
		}
	})

	t.Run("server stream", func(t *testing.T) {
		stream, err := h.conn.NewStream(ctx, &usersDesc.Streams[0], "/forgotest.Users/Watch")
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.SendMsg(newMessage(t, "GetUserRequest", `{"id": "u-1"}`)); err != nil {
			t.Fatal(err)
		}
		_ = stream.CloseSend()
		var ids []any
		for {
			m := dynamicpb.NewMessage(message("User"))
			if err := stream.RecvMsg(m); err != nil {
				break
			}
			ids = append(ids, userJSON(t, m)["id"])
		}
		if !jsonEqual(ids, `["u-1", "u-2", "u-3"]`) {
			t.Fatalf("ids %v", ids)
		}
	})

	t.Run("client stream", func(t *testing.T) {
		stream, err := h.conn.NewStream(ctx, &usersDesc.Streams[1], "/forgotest.Users/Import")
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"u-1", "u-2"} {
			if err := stream.SendMsg(newMessage(t, "User", `{"id": "`+id+`", "name": "x"}`)); err != nil {
				t.Fatal(err)
			}
		}
		_ = stream.CloseSend()
		report := dynamicpb.NewMessage(message("ImportReport"))
		if err := stream.RecvMsg(report); err != nil {
			t.Fatal(err)
		}
		if got := userJSON(t, report); !jsonEqual(got, `{"imported": 2, "ids": ["u-1", "u-2"]}`) {
			t.Fatalf("got %v", got)
		}
		if body := h.line(t, "operation completed")["resp_body"]; !jsonEqual(body, `{"imported": 2, "ids": ["u-1", "u-2"]}`) {
			t.Fatalf("resp_body %v", body)
		}

		// A request the validator rejects ends the range with its error.
		stream, err = h.conn.NewStream(ctx, &usersDesc.Streams[1], "/forgotest.Users/Import")
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SendMsg(newMessage(t, "User", `{"id": "u-1"}`))
		_ = stream.CloseSend()
		assertStatus(t, stream.RecvMsg(report), codes.InvalidArgument, "field: name, rule: required", "validation")
	})

	t.Run("bidi", func(t *testing.T) {
		stream, err := h.conn.NewStream(ctx, &usersDesc.Streams[2], "/forgotest.Users/Sync")
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"ada", "grace"} {
			if err := stream.SendMsg(newMessage(t, "User", `{"name": "`+name+`"}`)); err != nil {
				t.Fatal(err)
			}
			m := dynamicpb.NewMessage(message("User"))
			if err := stream.RecvMsg(m); err != nil {
				t.Fatal(err)
			}
			if got := userJSON(t, m)["name"]; got != strings.ToUpper(name) {
				t.Fatalf("got %v", got)
			}
		}
		_ = stream.CloseSend()
	})
}

// ptrText is an encoding.TextMarshaler through its pointer only.
type ptrText struct{ s string }

func (p *ptrText) MarshalText() ([]byte, error) { return []byte(strings.ToUpper(p.s)), nil }

func (p *ptrText) UnmarshalText(b []byte) error {
	p.s = strings.ToLower(string(b))
	return nil
}

// scalars maps every integer kind to an integer type of another size or sign.
type scalars struct {
	I32, F64, Sf64 int
	I64            int8
	U32            uint
	U64            uint8
	S32            int16
	S64            uint64
	F32            uint32
	Sf32           int32
	Fl, Db         float32
	List           []any
	Count          int // a wrapper by value
	Stamp          *timestamppb.Timestamp
	Names          map[userID]ptrText
}

func TestBindingScalars(t *testing.T) {
	b := mustBinding[scalars](t, message("Scalars"), true)
	got, err := b.toGo(context.Background(), newMessage(t, "Scalars", `{
		"i32": 1, "i64": "2", "u32": 3, "u64": "4", "s32": -5, "s64": "6", "f32": 7, "f64": "8", "sf32": -9, "sf64": "-10",
		"fl": 1.5, "db": 2.5, "list": [1, "a"], "count": "11", "stamp": "1970-01-01T00:00:01Z", "names": {"u-1": "ONE"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Stamp, timestamppb.New(time.Unix(1, 0))) {
		t.Fatalf("stamp %v", got.Stamp)
	}
	got.Stamp = nil
	want := scalars{
		I32: 1, I64: 2, U32: 3, U64: 4, S32: -5, S64: 6, F32: 7, F64: 8, Sf32: -9, Sf64: -10, Fl: 1.5, Db: 2.5,
		List: []any{1.0, "a"}, Count: 11, Names: map[userID]ptrText{{1}: {"one"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}

	for js, path := range map[string]string{
		`{"i64": "200"}`:                  "i64",
		`{"u64": "300"}`:                  "u64",
		`{"s64": "-1"}`:                   "s64",
		`{"s32": 70000}`:                  "s32",
		`{"f64": "18446744073709551615"}`: "f64",
		`{"db": 1e300}`:                   "db",
		`{"u32": 4294967295}`:             "",
	} {
		_, err := b.toGo(context.Background(), newMessage(t, "Scalars", js))
		var fe *fieldError
		if path == "" && err != nil || path != "" && (!errors.As(err, &fe) || fe.Path() != path) {
			t.Errorf("%s: got %v, want an error at %q", js, err, path)
		}
	}

	e := mustBinding[scalars](t, message("Scalars"), false)
	want.Stamp = timestamppb.New(time.Unix(1, 0))
	m, err := e.toProto(want)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := protojson.Marshal(m); !jsonEqual(json.RawMessage(got), `{
		"i32": 1, "i64": "2", "u32": 3, "u64": "4", "s32": -5, "s64": "6", "f32": 7, "f64": "8", "sf32": -9, "sf64": "-10",
		"fl": 1.5, "db": 2.5, "list": [1, "a"], "count": "11", "stamp": "1970-01-01T00:00:01Z", "names": {"u-1": "ONE"}
	}`) {
		t.Fatalf("got %s", got)
	}
	for _, s := range []scalars{{I32: math.MaxInt32 + 1}, {U32: math.MaxUint32 + 1}, {S64: math.MaxInt64 + 1}, {F64: -1}} {
		if _, err := e.toProto(s); !errors.Is(err, errInvalidResponse) {
			t.Errorf("%+v: got %v", s, err)
		}
	}
	type floats struct{ Fl float64 }
	f := mustBinding[floats](t, message("Scalars"), false)
	if _, err := f.toProto(floats{Fl: 1e300}); !errors.Is(err, errInvalidResponse) {
		t.Fatalf("got %v", err)
	}
	if _, err := f.toProto(floats{Fl: math.Inf(1)}); err != nil {
		t.Fatal(err)
	}
}

// Fields tagged header come from the call's metadata.
func TestBindingHeaders(t *testing.T) {
	type named string
	type headers struct {
		Street, ZipCode string
		B               bool     `header:"x-b"`
		I               int16    `header:"x-i"`
		U               uint8    `header:"x-u"`
		F               float32  `header:"x-f"`
		T               userID   `header:"x-t"`
		P               *int     `header:"x-p"`
		S               []string `header:"x-s"`
		N               named    `header:"X-N"`
		Missing         *string  `header:"x-missing"`
	}
	b := mustBinding[headers](t, message("Address"), true)
	call := func(pairs ...string) (headers, error) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
		return b.toGo(ctx, newMessage(t, "Address", `{"street": "Main"}`))
	}
	got, err := call("x-b", "true", "x-i", "-7", "x-u", "8", "x-f", "1.5", "x-t", "u-3", "x-p", "9",
		"x-s", "a", "x-s", "b", "x-n", "n")
	if err != nil {
		t.Fatal(err)
	}
	want := headers{Street: "Main", B: true, I: -7, U: 8, F: 1.5, T: userID{3}, P: ptr(9), S: []string{"a", "b"}, N: "n"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
	for _, pair := range [][2]string{{"x-b", "maybe"}, {"x-i", "70000"}, {"x-u", "-1"}, {"x-f", "x"}, {"x-t", "3"}, {"x-p", "p"}} {
		_, err := call(pair[0], pair[1])
		var fe *fieldError
		if !errors.As(err, &fe) || fe.source != sourceMetadata || fe.Path() != pair[0] {
			t.Errorf("%v: got %v", pair, err)
		}
	}
}

// A field holding a proto message gets a copy when the message is another implementation of it, here a
// dynamicpb.Message.
func TestBindingProtoField(t *testing.T) {
	type all struct {
		Stamp                                  *timestamppb.Timestamp
		I32, I64, U32, U64, S32, S64, F32, F64 int64
		Sf32, Sf64                             int64
		Fl, Db                                 float64
		List                                   []any
		Count                                  *int64 // a wrapper by pointer: set to 0 is not nil
		Names                                  map[string]string
	}
	got, err := mustBinding[all](t, message("Scalars"), true).toGo(context.Background(),
		newMessage(t, "Scalars", `{"stamp": "1970-01-01T00:00:02Z", "count": "0"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got.Stamp, timestamppb.New(time.Unix(2, 0))) || got.Count == nil || *got.Count != 0 {
		t.Fatalf("got %+v", got)
	}
}

// The method that serves forgotest.Users/Get over gRPC serves an HTTP route too, bound from the path, the
// query and the headers.
func TestSameMethodOverHTTP(t *testing.T) {
	boot := forgo.NewBootstrap(nil, []any{newUserService})
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = boot.Close() })
	addr := freeAddr(t)
	s, err := fhttp.Config{Addr: addr, Routes: []fhttp.RouteConfig{
		{Method: "GET", Path: "/users/{id}", Handler: fhttp.HandlerFor((*userService).Get)},
	}}.New(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Init(logger.NewCtx(context.Background(), zerolog.Nop())); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve() }()
	t.Cleanup(func() {
		_ = s.Terminate(context.Background())
		<-served
	})

	req, _ := http.NewRequestWithContext(testCtx(t), "GET", "http://"+addr+"/users/u-7?fields=name", nil)
	req.Header.Set("X-Tenant", "acme")
	var resp *http.Response
	for range 100 {
		if resp, err = http.DefaultClient.Do(req); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"name":"Ada"`) ||
		!strings.Contains(string(body), `"tenant":"acme"`) || !strings.Contains(string(body), `"tags":["name"]`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

// A validate tag the server does not know panics in New, however deep it is, rather than on the first call.
func TestUnknownValidateTag(t *testing.T) {
	type zip struct {
		Code string `validate:"zipcode"`
	}
	svc := func(h HandlerFactory) []ServiceConfig {
		return []ServiceConfig{{Desc: &usersDesc, Methods: []MethodConfig{{Method: "Get", Handler: h}}}}
	}
	type get struct {
		ID     string
		Fields []string
		Nested []map[string]*zip `grpc:"-"`
	}
	handler := HandlerAs[get, *user](func() Handler[get, *user] {
		return func(context.Context, get) (*user, error) { return &user{}, nil }
	})
	boot := forgo.NewBootstrap(nil, nil)
	r, err := boot.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = boot.Close() }()

	func() {
		defer func() {
			if r := recover(); !strings.Contains(fmt.Sprint(r), "Undefined validation function 'zipcode'") {
				t.Fatalf("recovered %v", r)
			}
		}()
		_, _ = Config{Services: svc(handler)}.New(r)
	}()
	zipcode := func(validator.FieldLevel) bool { return true }
	if _, err := (Config{Services: svc(handler), Validators: map[string]validator.Func{"zipcode": zipcode}}).New(r); err != nil {
		t.Fatal(err)
	}
	// A Validator of the configuration's own is not checked.
	if _, err := (Config{Services: svc(handler), Validator: func(any) error { return nil }}).New(r); err != nil {
		t.Fatal(err)
	}
	if _, err := (Config{Services: svc(handler), Validators: map[string]validator.Func{"": zipcode}}).New(r); err == nil {
		t.Fatal("registered a validator without a name")
	}
}
