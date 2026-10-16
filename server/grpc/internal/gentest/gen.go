//go:build ignore

// gen writes the Go code of the test schemas, the *.txtpb text descriptors next to it. It hands them to the
// protoc-gen-go of the protobuf module forgo already depends on, as protoc would, so that neither protoc nor
// buf is needed. Run it with go generate in this directory after changing a schema.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"google.golang.org/protobuf/types/pluginpb"
)

// schemas are the files to generate, and the API level of each.
var schemas = []struct {
	file     string
	apiLevel string
}{
	{"users.txtpb", "API_OPEN"},
	{"kinds.txtpb", "API_OPEN"},
	{"hybrid.txtpb", "API_HYBRID"},
	{"opaque.txtpb", "API_OPAQUE"},
	{"mixed.txtpb", "API_OPEN"},
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

func run() error {
	req := &pluginpb.CodeGeneratorRequest{Parameter: proto.String("paths=source_relative")}
	for _, fd := range []protoreflect.FileDescriptor{
		timestamppb.File_google_protobuf_timestamp_proto,
		durationpb.File_google_protobuf_duration_proto,
		wrapperspb.File_google_protobuf_wrappers_proto,
		structpb.File_google_protobuf_struct_proto,
		fieldmaskpb.File_google_protobuf_field_mask_proto,
	} {
		req.ProtoFile = append(req.ProtoFile, protodesc.ToFileDescriptorProto(fd))
	}
	for _, s := range schemas {
		text, err := os.ReadFile(s.file)
		if err != nil {
			return err
		}
		fdp := new(descriptorpb.FileDescriptorProto)
		if err := prototext.Unmarshal(text, fdp); err != nil {
			return fmt.Errorf("%s: %w", s.file, err)
		}
		req.ProtoFile = append(req.ProtoFile, fdp)
		req.FileToGenerate = append(req.FileToGenerate, fdp.GetName())
		*req.Parameter += ",apilevelM" + fdp.GetName() + "=" + s.apiLevel
	}
	if _, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.ProtoFile}); err != nil {
		return err
	}

	in, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	var out, stderr bytes.Buffer
	cmd := exec.Command("go", "run", "google.golang.org/protobuf/cmd/protoc-gen-go")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = bytes.NewReader(in), &out, &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("protoc-gen-go: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	resp := new(pluginpb.CodeGeneratorResponse)
	if err := proto.Unmarshal(out.Bytes(), resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("protoc-gen-go: %s", resp.GetError())
	}
	for _, f := range resp.File {
		if err := os.WriteFile(path.Base(f.GetName()), []byte(f.GetContent()), 0o644); err != nil {
			return err
		}
	}
	return nil
}
