package main

import (
	api "Wx_MQ/kitex_gen/api/client_operations"
	"log"
)

func main() {
	svr := api.NewServer(new(Client_OperationsImpl))

	err := svr.Run()

	if err != nil {
		log.Println(err.Error())
	}
}
