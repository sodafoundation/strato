package main

import (
	"fmt"
	"os"

	micro "github.com/micro/go-micro"
	"github.com/opensds/go-panda/backend/pkg/db"
	handler "github.com/opensds/go-panda/backend/pkg/service"
	"github.com/opensds/go-panda/backend/pkg/utils/config"
	pb "github.com/opensds/go-panda/backend/proto"
)

func main() {
	dbHost := os.Getenv("DB_HOST")
	db.Init(&config.Database{
		Driver:   "mongodb",
		Endpoint: dbHost})
	defer db.Exit()

	service := micro.NewService(
		micro.Name("backend"),
	)

	service.Init()

	pb.RegisterBackendHandler(service.Server(), handler.NewBackendService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
