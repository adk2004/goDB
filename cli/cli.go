package main

import (
	"fmt"

	"github.com/adk2004/goDB/db/engine"
)

func main() {
	fmt.Println("Starting goDB CLI...")
	db, err := engine.NewEngine("./db_data", 100)
	if err != nil {
		fmt.Printf("Failed to initialize database: %v\n", err)
		panic(err)
	}
	defer db.Close()
	for {
        var cmd, key, value string
        fmt.Scanln(&cmd, &key, &value)
        switch cmd {
        case "put":
			if len(key) == 0 || len(value) == 0 {
				fmt.Println("<key> and <value> cannot be empty for put command")
				continue
			}
            err := db.Put(key, []byte(value))
			if err != nil {
				fmt.Printf("Put Error : %v\n", err)
			}
        case "get":
            v, err := db.Get(key)
			if err != nil {
				fmt.Printf("Get Error : %v\n", err)
			} else {
            	fmt.Println(string(v))
			}
        case "delete":
            err := db.Delete(key)
			if err != nil {
				fmt.Printf("Delete Error : %v\n", err)
			}
		case "list":
			keys := db.List()
			fmt.Println(keys)
		case "exit":
			return
		default:
			fmt.Println("Unknown command.\nAvailable commands: put <key> <value>, get <key>, delete <key>, list, exit")
        }
    }
}
