package forgo_test

import (
	"fmt"

	"github.com/efegreat/forgo"
)

// Greeter is the port Welcomer needs. Any provider whose result implements it satisfies it.
type Greeter interface {
	Greet(name string) string
}

type englishGreeter struct{}

func (englishGreeter) Greet(name string) string { return "hello, " + name }

func NewEnglishGreeter() englishGreeter { return englishGreeter{} }

type Welcomer struct {
	greeter Greeter
}

func NewWelcomer(g Greeter) *Welcomer { return &Welcomer{greeter: g} }

func (w *Welcomer) Welcome(name string) string { return w.greeter.Greet(name) + ", welcome aboard" }

func ExampleBootstrap() {
	// Providers are called in declaration order, so NewEnglishGreeter comes before the
	// constructor that needs a Greeter.
	b := forgo.NewBootstrap(nil, []any{NewEnglishGreeter, NewWelcomer})
	r, err := b.Build()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer b.Close()

	// Server factories resolve their dependencies like this, before the bootstrap is sealed.
	w, err := r.Resolve[*Welcomer]()
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := b.Seal(); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(w.Welcome("Ada"))
	// Output: hello, Ada, welcome aboard
}
