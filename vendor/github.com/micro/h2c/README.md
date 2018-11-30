# h2c
#### Golang HTTP/2 cleartext (h2c) Handler / Router / Mux

The h2c protocol is the non-TLS secured version of HTTP/2 which is not available from net/http.

Code is a copy of Traefik's h2c server, but adapted for standalone usage as an http.Handler.

Traefik can be found here: github.com/containous/traefik

#

### Please do not use this library unless you know what you are doing, and you have an appropriate use case such as a secure load balancer that terminates SSL before sending h2c traffic to servers within a private subnet.

#

Example usage:

```go
package main

import (
	"fmt"
	"net/http"

	"github.com/veqryn/h2c"
	"golang.org/x/net/http2"
)

func main() {

	router := http.NewServeMux()

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello World")
	})

	h2cWrapper := &h2c.HandlerH2C{
		Handler:  router,
		H2Server: &http2.Server{},
	}

	srv := http.Server{
		Addr:    ":8080",
		Handler: h2cWrapper,
	}

	srv.ListenAndServe()
}
```
