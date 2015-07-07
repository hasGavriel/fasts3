package s3wrapper

import (
    "io/ioutil"
    "log"
    "os"
    "strings"
    "sync"
    "time"

    "github.com/AdRoll/goamz/s3"
)

const defaultDelimiter string = "/"

const numListRoutines int = 30

const maxRetries int = 5

type listWork struct {
    prefix    string
    delimiter string
}

// StripS3Path gets the key part from an s3 uri. The key part is everything after
// the header (s3://<bucket>)
func StripS3Path(key string) string {
    key = strings.TrimRight(key, "\n")
    if strings.Index(key, "s3://") < 0 {
        return key // IF s3:// is not located, assume the file is already stripped
    }

    // Take the first element after splitting from tab (Should be 's3://.../.../.../...')
    metaFilePath := strings.Split(key, "\t")[0]
    // Clear everything off the line before 's3://' to allow taking s3cmd ls output as input
    s3Path := metaFilePath[strings.Index(metaFilePath, "s3://"):]
    // The bucket is the second element if you split the string 's3://<bucket>/' by '/'
    bucket := strings.Split(s3Path, "/")[2]
    header := "s3://" + bucket
    return metaFilePath[strings.Index(metaFilePath, header)+len(header):]
}

// getListWork generates a list of prefixes based on prefix by searching down the searchDepth
// using DELIMITER as a delimiter
func getListWork(bucket *s3.Bucket, prefix string, searchDepth int) []listWork {
    currentPrefixes := []listWork{listWork{prefix, ""}}
    results := []listWork{}
    for i := 0; i < searchDepth; i++ {
        newPrefixes := []listWork{}
        for _, pfx := range currentPrefixes {
            for res := range List(bucket, pfx.prefix, defaultDelimiter) {
                if len(res.CommonPrefixes) != 0 {
                    for _, newPfx := range res.CommonPrefixes {
                        newPrefixes = append(newPrefixes, listWork{newPfx, ""})
                    }
                    // catches the case where keys and common prefixes live in the same place
                    if len(res.Contents) > 0 {
                        results = append(results, listWork{pfx.prefix, "/"})
                    }
                } else {
                    results = append(results, listWork{pfx.prefix, ""})
                }
            }
        }
        currentPrefixes = newPrefixes
    }
    for _, pfx := range currentPrefixes {
        results = append(results, pfx)
    }
    return results
}

// List function with retry and support for listing all keys in a prefix
func List(bucket *s3.Bucket, prefix string, delimiter string) <-chan *s3.ListResp {
    ch := make(chan *s3.ListResp, 100)
    go func(pfix string, del string) {
        defer close(ch)
        isTruncated := true
        nextMarker := ""
        for isTruncated {
            attempts := 0
            for {
                attempts++
                res, err := bucket.List(pfix, del, nextMarker, 1000)
                if err != nil {
                    if err.Error() == "runtime error: index out of range" {
                        break
                    }
                    if attempts >= maxRetries {
                        log.Panic(err)
                    }

                    time.Sleep(time.Second * 3)
                } else {
                    ch <- res
                    if len(res.Contents) > 0 {
                        nextMarker = res.Contents[len(res.Contents)-1].Key
                    } else if len(res.CommonPrefixes) > 0 {
                        nextMarker = res.CommonPrefixes[len(res.CommonPrefixes)-1]
                    }
                    isTruncated = res.IsTruncated
                    break
                }
            }
        }
    }(prefix, delimiter)
    return ch
}

func Put(bucket *s3.Bucket, key string, contents []byte, contentType string, permissions s3.ACL, options s3.Options) error {
    attempts := 0
    for {
        attempts++
        err := bucket.Put(key, contents, contentType, permissions, options)
        if err == nil {
            return nil
        }
        if attempts >= maxRetries && err != nil {
            return err
        }

        time.Sleep(time.Second * 3)
    }
}

func Get(bucket *s3.Bucket, key string) ([]byte, error) {
    attempts := 0
    for {
        attempts++
        buff, err := bucket.Get(key)
        if err == nil {
            return buff, nil
        }
        if attempts >= maxRetries && err != nil {
            return nil, err
        }
    }

}

// createPathIfNotExists takes a path and creates
// it if it doesn't exist
func createPathIfNotExists(path string) error {
    _, err := os.Stat(path)
    if os.IsNotExist(err) {
        err := os.MkdirAll(path, 0755)
        if err != nil {
            return err
        }
    } else {
        return nil
    }
    return nil
}

// GetToFile takes a bucket and key and puts the bytes to a file
// in specified by dest
func GetToFile(bucket *s3.Bucket, key string, dest string) error {
    destParts := strings.Split(dest, "/")
    path := strings.Join(destParts[0:len(destParts)-1], "/")

    if err := createPathIfNotExists(path); err != nil {
        return err
    }

    bts, err := Get(bucket, key)
    if err != nil {
        return err
    }
    err = ioutil.WriteFile(dest, bts, 0644)
    if err != nil {
        return err
    }
    return nil
}

func toDeleteStruct(keys []string) s3.Delete {
    objs := make([]s3.Object, 0)
    for _, key := range keys {
        if key != "" {
            objs = append(objs, s3.Object{Key: strings.TrimSpace(key)})
        }
    }
    return s3.Delete{false, objs}
}

// DeleteMulti  deletes multiple keys
func DeleteMulti(bucket *s3.Bucket, keys []string) error {
    attempts := 0
    for {
        attempts++
        err := bucket.DelMulti(toDeleteStruct(keys))
        if err != nil {
            if attempts >= maxRetries {
                return err
            }

            time.Sleep(time.Second * 3)
        } else {
            break
        }
    }
    return nil
}


func ListMain(bucket *s3.Bucket, prefix string, searchDepth int, isRecursive bool) chan s3.Key {
    chResults := make(chan s3.Key, 1000)    // Channel for results
    const MAX_SIMULTANEOUS_WORKERS = 100                    // limit the number of GoListRecurse workers
    chThrottle := make(chan int, MAX_SIMULTANEOUS_WORKERS)  // every worker adds a msg to the channel at start and removes one at end
    var wg sync.WaitGroup // Wait group counter for go-routines - invokers add 1, goroutines declare done before exiting
    wg.Add(1)             // This is in anticipation of our first go GoListRecurse (and needed here to prevent premature channel closure)
    go func () {          // Monitor worker completion - close channel when everyone's done writing
        wg.Wait()
        close(chResults)
    }()
    go goListRecurse(bucket, prefix, searchDepth, isRecursive, chResults, chThrottle, &wg)
    return chResults;
}


// lists a prefix and includes common prefixes
func depreciated_ListWithCommonPrefixes(bucket *s3.Bucket, prefix string) chan s3.Key {
    ch := make(chan s3.Key, 1000)
    go func() {
        defer close(ch)
        for listResp := range List(bucket, prefix, defaultDelimiter) {
            for _, prefix := range listResp.CommonPrefixes {
                ch <- s3.Key{prefix, "", -1, "", "", s3.Owner{}}
            }
            for _, key := range listResp.Contents {
                ch <- key
            }
        }
    }()
    return ch
}

// min function for integers
func intMin(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func intMax(x, y int) int {
    if x < y {
        return y
    }
    return x
}

// partition partitions list into list of lists where len(lists) <= partitions
func partition(list []listWork, partitionSize int) [][]listWork {
    partitions := [][]listWork{}
    step := intMax(len(list)/partitionSize, 1)
    for i := 0; i < len(list); i += step {
        outerBound := intMin(len(list), i+step)
        partitions = append(partitions, list[i:outerBound])
    }
    return partitions
}

// goListRecurse lists prefixes in parallel using searchDepth to control forking
func goListRecurse(bucket *s3.Bucket, prefix string, searchDepth int, isRecursive bool, chResults chan s3.Key, chThrottle chan int, wg *sync.WaitGroup) {
    defer wg.Done()
    defer func() {
        <- chThrottle
    }()
    chThrottle <- 1     // This will block if too many workers
    myDelimiter := defaultDelimiter
    if searchDepth < 1 && isRecursive {
        myDelimiter = ""
    }
    for res := range List(bucket, prefix, myDelimiter) {
        if len(res.CommonPrefixes) != 0 {
            if searchDepth > 0 {
                for _, newPfx := range res.CommonPrefixes {
                    wg.Add(1)
                    go goListRecurse(bucket, newPfx, searchDepth-1, isRecursive, chResults, chThrottle, wg)
                }
            } else {
                for _, newPfx := range res.CommonPrefixes {
                    chResults <- s3.Key{newPfx, "", -1, "", "", s3.Owner{}}
                }
            }
        }
        if len(res.Contents) > 0 {
            for _, key := range res.Contents {
                chResults <- key
            }
        }
    }
}

// listRecurse lists prefix in parallel using searchDepth to search for routine's work
func ListRecurse(bucket *s3.Bucket, prefix string, searchDepth int) chan s3.Key {
    ch := make(chan s3.Key, 2000)
    var wg sync.WaitGroup

    workPartitions := partition(getListWork(bucket, prefix, searchDepth), numListRoutines)
    for _, partition := range workPartitions {
        wg.Add(1)
        go func(work []listWork) {
            defer wg.Done()
            for _, workItem := range work {
                for res := range List(bucket, workItem.prefix, workItem.delimiter) {
                    for _, c := range res.Contents {
                        ch <- c
                    }
                }
            }
        }(partition)
    }

    go func() {
        wg.Wait()
        close(ch)
    }()
    return ch
}

