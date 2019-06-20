## Quick Start for Learning How OpenSDS Multi-cloud Client Works

To better learn how opensds multi-cloud client works for connecting with OpenSDS 
multi-cloud service, here is a two-step example showing how to use the client.

Before these two steps, you have to make sure client package has been imported
in your local repo.

### Step 1: Initialize Client object
It's a simple and easy step for user to create a new Client object like below:
```go
package main

import (
	"fmt"
	
	"github.com/opensds/multi-cloud/client"
)

func main() {
	c := client.NewClient(&client.Config{
		Endpoint: "http://127.0.0.1:8089",
	})
	
	fmt.Printf("c is %v\n", c)
}
```

### Step 2: Call method in Client object
In the second step, you can just call method in Client object which is created
in step 1 like this:
```go
package main

import(
	"fmt"
	
	"github.com/opensds/multi-cloud/client"
)

func main() {
	c := client.NewClient(&client.Config{
		Endpoint: "http://127.0.0.1:8089",
	})
	
	backends, err := client.ListBackends()
	if err != nil {
		fmt.Println(err)
	}
	
	fmt.Printf("backends is %+v\n", backends)
}
```
