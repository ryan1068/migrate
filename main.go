package main

import (
	"flag"
	"migrate/model/shop"
	"reflect"
)

func main() {
	mode := flag.String("mode", "", "mode type")
	ac := flag.String("ac", "", "create or drop tables")
	flag.Parse()

	if exists, _ := InSlice(*mode, []string{"tables", "migrate"}); !exists {
		panic("mode input error")
	}

	images := &shop.SmartFlowImage{}
	if *mode == "tables" {
		if exists, _ := InSlice(*mode, []string{"create", "drop"}); !exists {
			panic("ac input error")
		}
		images.HandleTable(*ac)
	} else if *mode == "migrate" {
		images.Migrate()
	}

}

func InSlice(val interface{}, array interface{}) (exists bool, index int) {
	exists = false
	index = -1

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				index = i
				exists = true
				return
			}
		}
	}

	return
}
