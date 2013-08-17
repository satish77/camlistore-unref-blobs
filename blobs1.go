package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	//"reflect"
	"sort"
	//"syscall"
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
	camliContent BlobRef
	camliType    int
	constant     string
	entries      string
	members      []string
	parts        []PartType
	permaNode    BlobRef
	path         string
}

type PartType struct {
	sha1 string
	size float64
}

var camliTypes map[string]int
var blobs map[BlobRef]BlobType

func camliTypeLookup(value int) string {
	for k, v := range camliTypes {
		if v == value {
			return k
		}
	}
	return "binary"
}

func fileToBlobRef(camliFile string) BlobRef {
	return BlobRef(camliFile[0 : len(camliFile)-len(filepath.Ext(camliFile))])
}

func printCamliTypes() {
	TypeCnt := make(map[int]int)

	for blobref := range blobs {
		TypeCnt[blobs[blobref].camliType]++
	}
	for camliType := range TypeCnt {
		fmt.Println(camliTypeLookup(camliType), TypeCnt[camliType])
	}
}

func printCamliFilesByType(camliType string) {
	fmt.Println(camliType)
	camliTypeInt, _ := camliTypes[camliType]
	for blob := range blobs {
		if blobs[blob].camliType == camliTypeInt {
			fmt.Println(blobs[blob].path)
		}
	}
}

func processCamliFile(path string) {
	//fmt.Println(path)
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	var b BlobType
	b.path = path
	filename := filepath.Base(path)
	blobRef := BlobRef(filename[0 : len(filename)-len(filepath.Ext(filename))])
	//fmt.Println(blobRef)

	pattern := "{\"camliVersion\": 1,"
	if string(content[0:len(pattern)]) != pattern {
		//fmt.Println("Binary: ", path)
		blobs[blobRef] = b
		return
	}

	u := map[string]interface{}{}
	err = json.Unmarshal(content, &u)
	if err != nil {
		panic(err)
	}

	camliType := u["camliType"].(string)

	var ok bool
	b.camliType, ok = camliTypes[camliType]
	if !ok {
		panic(camliType + " not found.")
	}

	if u["entries"] != nil {
		b.entries = u["entries"].(string)
	}

	if u["parts"] != nil {
		//fmt.Println(u["parts"])
		//fmt.Println(reflect.TypeOf(u["parts"]))
		for _, v := range u["parts"].([]interface{}) {
			//fmt.Println(v)
			partBlobRef := v.(map[string]interface{})["blobRef"]
			partSize := v.(map[string]interface{})["size"]
			if partBlobRef == nil {
				partBlobRef = v.(map[string]interface{})["bytesRef"]
				//fmt.Println("bytesRef: " + v.(map[string]interface{})["bytesRef"].(string))
			}
			//fmt.Printf("%T %T\n", partBlobRef, partSize) // string float64
			b.parts = append(b.parts, (PartType{sha1: partBlobRef.(string), size: partSize.(float64)}))
		}
	}

	if u["members"] != nil {
		//fmt.Println(u["members"])
		//fmt.Printf("%T\n", u["members"].([]interface{}))
		for _, member := range u["members"].([]interface{}) {
			//fmt.Printf("%T\n", member)
			b.members = append(b.members, member.(string))
		}
		//fmt.Println(b.members)
	}

	if u["attribute"] != nil {
		if u["attribute"].(string) == "camliContent" {
			b.camliContent = BlobRef(u["value"].(string))
		}
	}

	if u["permaNode"] != nil {
		b.permaNode = BlobRef(u["permaNode"].(string))
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
	if !fi.IsDir() {
		camliFileChan <- path
	}

	return nil
}

var BlobRefCount map[BlobRef]int

func WalkBlob(blobRef BlobRef) {
	if blobRef == "" {
		return
	}
	//fmt.Println("WalkBlob: ", blobRef, blobs[blobRef].camliType)

	BlobRefCount[blobRef]++

	switch blobs[blobRef].camliType {
	case claim:
		WalkBlob(blobs[blobRef].permaNode)
		WalkBlob(blobs[blobRef].camliContent)
	case static_set:
		for _, member := range blobs[blobRef].members {
			WalkBlob(BlobRef(member))
		}
	case file:
		for _, part := range blobs[blobRef].parts {
			WalkBlob(BlobRef(part.sha1))
		}
	case bytes:
		for _, part := range blobs[blobRef].parts {
			WalkBlob(BlobRef(part.sha1))
		}
	case directory:
		WalkBlob(BlobRef(blobs[blobRef].entries))
	case permanode:
	}
}

func RemovePermanode(removedNode BlobRef) {
	// initialize ref count to zero
	for k, _ := range blobs {
		BlobRefCount[k] = 0
	}
	for k, _ := range blobs {
		if blobs[k].camliType == claim {
			//fmt.Println(blobs[k].camliType, blobs[k])
			if blobs[k].permaNode != removedNode {
				WalkBlob(k)
			}
		}
	}
}

func printUnrefBlobs() {
	fmt.Println("Unreferenced Blobs:-")
	unrefCount := 0
	/* print the blobs that can be deleted */
	for blobRef := range BlobRefCount {
		//fmt.Println(blobRef, BlobRefCount[blobRef], camliTypeLookup(blobs[blobRef].camliType))
		if BlobRefCount[blobRef] == 0 {
			//fmt.Println(blobRef)
			unrefCount++
		}
	}
	fmt.Println(unrefCount, "of", len(BlobRefCount), "can be removed.")
}

func blobRefHist() {
	// create histogram
	hist := make(map[int]int)
	for blobRef := range BlobRefCount {
		hist[BlobRefCount[blobRef]]++
	}

	// print histogram
	fmt.Println("Histogram: count -> number of blobs")
	for k, v := range hist {
		fmt.Println(k, v)
	}

	// extract keys
	keys := make([]int, 0)
	for freq, _ := range hist {
		keys = append(keys, freq)
	}

	fmt.Println("Histogram: count(sorted) -> number of blobs")
	// sort keys and print
	sort.Ints(keys)
	for key := range keys {
		fmt.Println(keys[key], hist[keys[key]])
	}
}

func main() {
	flag.Parse()
	root := flag.Arg(0)

	//appdata, _ := syscall.Getenv("APPDATA")
	//root = appdata + "\\camlistore\\blobs\\sha1"

	blobs = make(map[BlobRef]BlobType)

	camliTypes = make(map[string]int)
	camliTypes["file"] = file
	camliTypes["bytes"] = bytes
	camliTypes["static-set"] = static_set
	camliTypes["directory"] = directory
	camliTypes["permanode"] = permanode
	camliTypes["claim"] = claim

	camliFileChan = make(chan string)

	go processCamliFileTask()

	if filepath.Walk(root, visit) == nil {
		camliFileChan <- "quit"
		printCamliFilesByType("permanode")
		printCamliFilesByType("claim")
		printCamliTypes()
	}

	BlobRefCount = make(map[BlobRef]int)
	//RemovePermanode("sha1-8c0d7c406bf39adb4903b8bdb7263c0680d4a03c")
	RemovePermanode("")
	printUnrefBlobs()
	//blobRefHist()

	fmt.Println("Finished.")
}
