package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	//"reflect"
	"syscall"
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

type camliTypeMap map[string]string

var camliFiles map[string]camliTypeMap
var camliTypesCnt map[string]int
var reachableBlobs map[string]int

var blobs map[BlobRef]BlobType

func fileToBlobRef(camliFile string) BlobRef {
	return BlobRef(camliFile[0 : len(camliFile)-len(filepath.Ext(camliFile))])
}

func printCamliTypes() {
	for camliType := range camliTypesCnt {
		fmt.Println(camliTypes[camliType], camliType, camliTypesCnt[camliType])
	}
}

func printCamliFilesByType(camliType string) {
	fmt.Println(camliType)
	for camliFile := range camliFiles[camliType] {
		fmt.Println(blobs[fileToBlobRef(camliFile)].path)
	}
}

func processCamliFile(path string) {
	//fmt.Println(path)
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}

	u := map[string]interface{}{}
	err = json.Unmarshal(content, &u)
	if err != nil {
		panic(err)
	}

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

	filename := filepath.Base(path)

	blobRef := BlobRef(filename[0 : len(filename)-len(filepath.Ext(filename))])
	//fmt.Println(blobRef)

	var b BlobType
	//b.sha1 = blobRef
	b.path = path

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

	if u["permanode"] != nil {
		b.permaNode = u["permanode"].(BlobRef)
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

	//  pattern := "{\"camliVersion\": 1,"
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
	//return
	if blobRef == "" {
		return
	}
	//fmt.Println(blobRef, blobs[blobRef].camliType)

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

func DeletePermanode(deletednode BlobRef) {
	for k, _ := range blobs {
		//fmt.Println(k)
		if (blobs[k].camliType == claim) && (blobs[k].permaNode != deletednode) {
			WalkBlob(k)
		}
	}
	/* print the blobs that can be deleted */
	for blobRef := range BlobRefCount {
		fmt.Println(blobRef, BlobRefCount[blobRef], blobs[blobRef].camliType)
		if BlobRefCount[blobRef] == 0 {
			fmt.Println(blobRef + " deleted")
		}
	}
}

var camliTypes map[string]int

func main() {
	flag.Parse()
	root := flag.Arg(0)

	appdata, _ := syscall.Getenv("APPDATA")
	root = appdata + "\\camlistore\\blobs\\sha1"

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
	DeletePermanode("sha1-8c0d7c406bf39adb4903b8bdb7263c0680d4a03c")

	fmt.Println("Finished.")
}
