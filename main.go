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
	"io/ioutil"
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
	log.Printf("Searching for all the files in path %s with extension .gz and File name prefix test1", FileRootPath)

    err := filepath.Walk(FileRootPath, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) != ".gz" {
			return nil
		}
		if(!strings.HasPrefix(info.Name(), "test1")){
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
	if(len(split) >= 3){
		json.Unmarshal([]byte(split[2]), &op)
	}else {
		log.Printf("Incorrect Data %v", data)
	}
	return op
}

func IngestData(obj interface{}){
	reqBody, _ := json.Marshal(obj)
	url := fmt.Sprintf("%s%s", endpoint, bulkLoadUrl)
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
var bulkLoadUrl string
var getUserIdUrl string
var clientUserIdToUserIdMap map[string]string = make(map[string]string)

func getUserId(clientUserId string, eventTimestamp int64) (string, error) {
	userId, found := clientUserIdToUserIdMap[clientUserId]
	if !found {
		// Create a user.
		userRequestMap := make(map[string]interface{})
		userRequestMap["c_uid"] = clientUserId
		userRequestMap["join_timestamp"] = eventTimestamp

		reqBody, _ := json.Marshal(userRequestMap)
		url := fmt.Sprintf("%s%s", endpoint, getUserIdUrl)
		client := &http.Client{}
		req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(reqBody))
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Add("Authorization", authToken)
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(fmt.Sprintf(
				"Http Post user creation failed. Url: %s, reqBody: %s, response: %+v, error: %+v", url, reqBody, resp, err))
			return "", err
		}
		// always close the response-body, even if content is not required
		defer resp.Body.Close()
		jsonResponse, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal("Unable to parse http user create response.")
			return "", err
		}
		var jsonResponseMap map[string]interface{}
		json.Unmarshal(jsonResponse, &jsonResponseMap)
		userId = jsonResponseMap["user_id"].(string)
		clientUserIdToUserIdMap[clientUserId] = userId
	}
	return userId, nil
}

func main(){
	// TODO : Check how log can be initialized to a file
	//GET these variables from Config
	root := "."
	maxBatchSize := 1000
	counter := 0
	var events []output
	endpoint = ""
	authToken = ""
	bulkLoadUrl = "/sdk/event/track/bulk"
	getUserIdUrl = "/sdk/user/identify"
	files := GetAllUnreadFiles(root)
	log.Printf("Files to be processed %v", files)

	for _, element := range files {
		log.Printf("Processing contents of File: %s", element)
		zipReader := GetFileHandle(fmt.Sprintf("%s/%s",root,element))
		scanner := bufio.NewScanner(zipReader)
		for scanner.Scan() {
			s := scanner.Text()
			fmt.Println(s)
			op := ExtractData(s).(output)
			if(op.UserId != ""){
				op.UserId, _ = getUserId(op.UserId, (int64)(op.Timestamp))
				events = append(events, op)				
			}
			counter++ 

			if(counter == maxBatchSize){
				log.Printf("Processing %v records", len(events))
				IngestData(events)
				counter = 0
				events = nil
			}
		}
		if(counter != 0){
			log.Printf("Processing %v records", len(events))
			IngestData(events)
			counter = 0
			events = nil
		}
		log.Printf("Done !!! Processing contents of File: %s", element)
		// TODO: Purging Files or moving it to poisson queue
		// These files will automatically be cleaned up after 28 days as set in the dataGen pipeline

	} 
}
