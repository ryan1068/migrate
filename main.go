package main

import (
	"flag"
	"migrate/model/shop"
)

// main 程序主体
func main() {
	mode := flag.String("mode", "", "mode type")
	ac := flag.String("ac", "", "create or drop tables")
	flag.Parse()

	if *mode != "tables" && *mode != "migrate" {
		panic("运行参数有误，请重新输入")
	}

	images := &shop.SmartFlowImage{}
	if *mode == "tables" {
		if *ac != "create" && *ac != "drop" {
			panic("操作表参数有误，请重新输入")
		}
		images.CreateTables(*ac)
	} else if *mode == "migrate" {
		images.Migrate()
	}

}
