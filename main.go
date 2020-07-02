package main

import(	
	"os"
	"fmt"
	"strings"
	"net/http"
	"bytes"
	"encoding/json"
	"compress/gzip"
	"bufio"
	"path/filepath"
	"log"
)

type output struct {
	UserId string `json:"user_id"`
	Event string `json:"event_name"`
	Timestamp int `json:"timestamp"`
	UserAttributes map[string]string `json:"user_properties"`
	EventAttributes map[string]string `json:"event_properties"`
}

func GetAllUnreadFiles(FileRootPath string)[]string {

	var files []string
	log.Printf("Searching for all the files in path %s with extension .gz and File name prefix outputLog", FileRootPath)

    err := filepath.Walk(FileRootPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) != ".gz" {
			return nil
		}
		if(!strings.HasPrefix(info.Name(), "outputLog")){
			return nil
		}
        files = append(files, path)
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
    return files
}

func GetFileHandle(FilePath string) (*gzip.Reader){
	log.Printf("ReadingFile %s", FilePath)
	handle, err := os.Open(FilePath)
	if err != nil {
		log.Fatal(err)
	}
	zipReader, err := gzip.NewReader(handle)
	if err != nil {
		log.Fatal(err)
	}
	defer zipReader.Close()
	return zipReader
}

func ExtractData(data string) interface{} {
	split := strings.Split(data, " ")
	var op output
	json.Unmarshal([]byte(split[2]), &op)
	return op
}

func IngestData(obj interface{}){
	reqBody, _ := json.Marshal(obj)
	url := fmt.Sprintf("%s%s", endpoint, url)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("Authorization", authToken)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		//TODO: Janani Handle Retry
		log.Fatal(err)
	}
	fmt.Println(resp)
}

var endpoint string
var authToken string
var url string

func main(){
	// TODO : Check how log can be initialized to a file
	//GET these variables from Config
	root := "."
	maxBatchSize := 1
	counter := 0
	var events []output
	endpoint = ""
	authToken = ""
	url = ""
	files := GetAllUnreadFiles(root)
	log.Printf("Files to be processed %v", files)

	for _, element := range files {
		log.Printf("Processing contents of File: %s", element)
		zipReader := GetFileHandle(fmt.Sprintf("%s/%s",root,element))
		scanner := bufio.NewScanner(zipReader)
		for scanner.Scan() {
			s := scanner.Text()
			events = append(events, ExtractData(s).(output))
			counter++ 

			if(counter == maxBatchSize){
				log.Printf("Processing %v records", len(events))
				IngestData(events)
				counter = 0
				events = nil
			}
		}
		log.Printf("Done !!! Processing contents of File: %s", element)
		// TODO: Purging Files or moving it to poisson queue
		// These files will automatically be cleaned up after 28 days as set in the dataGen pipeline
	} 
}
