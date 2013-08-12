package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	//"reflect"
)

// camliTypes
const (
	file       = 1
	bytes      = 2
	static_set = 3
	directory  = 4
	permanode  = 5
	claim      = 6
)

type BlobRef string

type BlobType struct {
	sha1         BlobRef
	camliContent BlobRef
	camliType    int
	constant     string
	entries      BlobRef
	parts        []PartType
	permanode    BlobRef
}

type PartType struct {
	sha1 string
	size float64
}

type camliTypeMap map[string]string

var camliFiles map[string]camliTypeMap
var camliTypesCnt map[string]int
var reachableBlobs map[string]int

var blobs map[BlobRef]BlobType

func printCamliTypes() {
	for camliType := range camliTypesCnt {
		fmt.Println(camliType, camliTypesCnt[camliType])
	}
}

func printCamliFilesByType(camliType string) {
	fmt.Println(camliType)
	for camliFile := range camliFiles[camliType] {
		fmt.Println(camliFile)
	}
}

func processCamliFile(path string) {
	fmt.Println(path)
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	// load json
	u := map[string]interface{}{}
	err = json.Unmarshal(content, &u)
	if err != nil {
		panic(err)
	}
	// Type assert values
	// Unmarshal stores "age" as a float even though it's an int.
	//fmt.Printf("Age: %1.0f\n", u["age"].(float64))
	//fmt.Printf("Married: %v\n", u["married"].(bool))

	camliType := u["camliType"].(string)

	if camliFiles == nil {
		camliFiles = make(map[string]camliTypeMap)
	}
	if camliFiles[camliType] == nil {
		camliFiles[camliType] = make(camliTypeMap)
	}
	camliFiles[camliType][filepath.Base(path)] = path

	if camliTypesCnt == nil {
		camliTypesCnt = make(map[string]int)
	}
	camliTypesCnt[camliType] = camliTypesCnt[camliType] + 1

	blobRef := BlobRef(filepath.Base(path))
	var b BlobType
	b.sha1 = blobRef
	if u["camliContent"] != nil {
		b.camliContent = u["camliContent"].(BlobRef)
	}
	b.camliType = camliTypes[camliType]
	if u["entries"] != nil {
		b.entries = BlobRef(u["entries"].(string))
	}

	if u["parts"] != nil {
		fmt.Println(u["parts"])
		//fmt.Println(reflect.TypeOf(u["parts"]))
		for _, v := range(u["parts"].([]interface{})) {
			fmt.Println(v)
			partBlobRef := v.(map[string]interface{})["blobRef"]
			partSize := v.(map[string]interface{})["size"]
			if partBlobRef == nil {
				partBlobRef = v.(map[string]interface{})["bytesRef"]
			}
			fmt.Printf("%T %T\n", partBlobRef, partSize) // string float64
			b.parts = append(b.parts, (PartType { sha1: partBlobRef.(string), size: partSize.(float64) }))
		}
	}
	if u["permanode"] != nil {
		b.permanode = u["permanode"].(BlobRef)
	}
	blobs[blobRef] = b
}

var camliFileChan chan string

func processCamliFileTask() {
	for {
		p := <-camliFileChan
		if p == "quit" {
			fmt.Println("quit received")
			return
		} else {
			processCamliFile(p)
		}
	}
}

func visit(path string, fi os.FileInfo, err error) error {
	if fi.IsDir() {
		return nil
	}

	if reachableBlobs == nil {
		reachableBlobs = make(map[string]int)
	}

	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() // f.Close will run when we're finished.

	//	pattern := "{\"camliVersion\": 1,"
	buf := make([]byte, 20)
	_, err = f.Read(buf[0:])

	if err == nil {
		firstLine := string(buf)
		//fmt.Printf("%v %s\n", buf, firstLine)
		if firstLine == "{\"camliVersion\": 1,\n" {
			//fmt.Printf("%s\n", fi.Name())
			//processCamliFile(path, fi.Name())
			reachableBlobs[fi.Name()] = 0
			camliFileChan <- path
		}
	}

	return nil
}

var BlobRefCount map[BlobRef]int

func WalkBlob(blobRef BlobRef) {
	if blobRef == "" {
		return
	}

	BlobRefCount[blobRef]++

	switch blobs[blobRef].camliType {
	case permanode:
		WalkBlob(blobs[blobRef].camliContent)
		/*	case static_set:
			case directory:
				for _, entry := range(blobs[blobRef].entries) {
					WalkBlob(entry)
				}*/
	case claim:
		if _, ok := blobs[blobs[blobRef].permanode]; ok {
			BlobRefCount[blobRef] = BlobRefCount[blobRef] + 1
		}
	}
}

func DeletePermanode(deletednode BlobRef) {
	for k, v := range blobs {
		if (v.camliType == permanode) && (v.sha1 != deletednode) {
			WalkBlob(v.sha1)
		}
		if blobs[k].camliType == claim {
			WalkBlob(v.sha1)
		}
	}
	/* print the blobs that can be deleted */
	for blobRef := range BlobRefCount {
		if BlobRefCount[blobRef] == 0 {
			fmt.Println(blobRef + " deleted")
		}
	}
}

var camliTypes map[string]int

func main() {
	flag.Parse()
	root := flag.Arg(0)

	blobs = make(map[BlobRef]BlobType)

	camliTypes = make(map[string]int)
	camliTypes["file"] = file
	camliTypes["static-set"] = static_set
	camliTypes["directory"] = directory
	camliTypes["permanode"] = permanode

	camliFileChan = make(chan string)

	go processCamliFileTask()

	if filepath.Walk(root, visit) == nil {
		camliFileChan <- "quit"
		//printCamliFilesByType("permanode")
		//printCamliFilesByType("directory")
		printCamliTypes()
	}

	fmt.Println("Finished.")
}
