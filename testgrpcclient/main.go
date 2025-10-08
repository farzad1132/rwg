package main

import (
	"context"
	"fmt"

	"github.com/farzad1132/rwg/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("192.168.1.100:3000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := protobuf.NewRajomonClientClient(conn)

	resp, err := client.SearchHotels(
		context.Background(),
		&protobuf.SearchHotelsRequest{
			Lat:     37.7867,
			Lon:     -122.4112,
			InDate:  "2024-08-15",
			OutDate: "2024-08-17",
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Response: %+v\n", resp)
}
