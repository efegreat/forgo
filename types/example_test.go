package types_test

import (
	"encoding/json"
	"fmt"

	"github.com/efegreat/forgo/types"
)

func ExampleNewMapCopyOnWrite() {
	// Static data shared by every operation, such as a route's args.
	routeArgs := map[string]string{"tenant": "acme"}

	op := types.NewMapCopyOnWrite(routeArgs)
	op.Set("tenant", "globex") // clones routeArgs first

	tenant, _ := op.Get("tenant")
	fmt.Println(tenant, routeArgs["tenant"])
	// Output: globex acme
}

func ExampleMap_GetTyped() {
	meta := types.NewJSONMap[any]()
	if err := meta.Scan(`{"retries":3,"id":18446744073709551615,"dry_run":true}`); err != nil {
		panic(err)
	}

	fmt.Println(meta.GetTyped[int]("retries"), meta.GetTyped[uint64]("id"), meta.GetTyped[bool]("dry_run"))
	fmt.Println(meta.GetTyped[string]("retries") == "", meta.GetTyped[int]("missing"))
	// Output:
	// 3 18446744073709551615 true
	// true 0
}

func ExampleJSONMap() {
	type event struct {
		Name string                 `json:"name"`
		Meta *types.JSONMap[string] `json:"meta"`
	}

	e := event{Name: "order.placed", Meta: types.NewJSONMapWithData(map[string]string{"tenant": "acme"})}
	encoded, _ := json.Marshal(e)
	fmt.Println(string(encoded))

	stored, _ := e.Meta.Value() // what database/sql writes to a JSON column
	fmt.Printf("%T %s\n", stored, stored)
	// Output:
	// {"name":"order.placed","meta":{"tenant":"acme"}}
	// string {"tenant":"acme"}
}
