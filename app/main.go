package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/olivere/elastic"
	"github.com/teris-io/shortid"
)

const (
	elasticIndexName = "documents"
	elasticTypeName  = "document"
)

type Document struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	Content   string    `json:"content"`
}

var (
	elasticClient *elastic.Client
)

type DocumentRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type DocumentResponse struct {
	ID        string
	CreatedAt time.Time
	Title     string `json:"title"`
	Content   string `json:"content"`
}

type SearchResponse struct {
	Time      string `json:"time"`
	Hits      string `json:"hit"`
	Documents []DocumentResponse
}

func errorResponse(c *gin.Context, code int, err string) {
	c.JSON(code, gin.H{
		"error": err,
	})
}

func couchGet(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		errorResponse(c, http.StatusBadRequest, "Query not specified")
		return
	}
	cl, err := couchbase.Connect("http://couchbase-master-service:8091")
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := cl.GetPool("default")
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot get pool")
		return
	}

	bucket, err := pool.GetBucket("default")
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot get bucket")
		return
	}
	var values interface{}
	err = bucket.Get(query, &values)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot insert into couchbase")
		return
	}
	c.JSON(http.StatusOK, values)
}

func couchInsert(c *gin.Context) {
	type request struct {
		Key    string
		Values []string
	}
	var postParams request
	if err := c.BindJSON(&postParams); err != nil {
		errorResponse(c, http.StatusBadRequest, "Malformed request body")
		return
	}
	cl, err := couchbase.Connect("http://couchbase-master-service:8091")
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}

	pool, err := cl.GetPool("default")
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot get pool")
		return
	}

	bucket, err := pool.GetBucket("default")
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot get bucket")
		return
	}

	err = bucket.Set(postParams.Key, 0, postParams.Values)
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "cannot insert into couchbase")
		return
	}
}

func createDocumentsEndpoint(c *gin.Context) {
	var docs []DocumentRequest
	if err := c.BindJSON(&docs); err != nil {
		errorResponse(c, http.StatusBadRequest, "Malformed request body")
		return
	}
	bulk := elasticClient.
		Bulk().
		Index(elasticIndexName).
		Type(elasticTypeName)
	for _, d := range docs {
		doc := Document{
			ID:        shortid.MustGenerate(),
			Title:     d.Title,
			CreatedAt: time.Now().UTC(),
			Content:   d.Content,
		}
		bulk.Add(elastic.NewBulkIndexRequest().Id(doc.ID).Doc(doc))
	}
	if _, err := bulk.Do(c.Request.Context()); err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Failed to create documents")
		return
	}
	c.Status(http.StatusOK)
}

func handler(c *gin.Context) {
	c.JSON(http.StatusOK, map[string]string{"status": "ok"})

}

func redisH(c *gin.Context) {
	client := redis.NewClient(&redis.Options{
		Addr:     "redis-master:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	err := client.Set("key", "value", 0).Err()
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "Failed to insert in redis")
		return
	}

	val, err := client.Get("key").Result()
	if err != nil {
		errorResponse(c, http.StatusInternalServerError, "Failed to get from redis")
		return
	}
	c.JSON(http.StatusOK, map[string]string{"key": val})
}

func searchEndpoint(c *gin.Context) {
	// Parse request
	query := c.Query("query")
	if query == "" {
		errorResponse(c, http.StatusBadRequest, "Query not specified")
		return
	}
	skip := 0
	take := 10
	if i, err := strconv.Atoi(c.Query("skip")); err == nil {
		skip = i
	}
	if i, err := strconv.Atoi(c.Query("take")); err == nil {
		take = i
	}
	esQuery := elastic.NewMultiMatchQuery(query, "title", "content").
		Fuzziness("2").
		MinimumShouldMatch("2")
	result, err := elasticClient.Search().
		Index(elasticIndexName).
		Query(esQuery).
		From(skip).Size(take).
		Do(c.Request.Context())
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Something went wrong")
		return
	}
	res := SearchResponse{
		Time: fmt.Sprintf("%d", result.TookInMillis),
		Hits: fmt.Sprintf("%d", result.Hits.TotalHits),
	}
	docs := make([]DocumentResponse, 0)
	for _, hit := range result.Hits.Hits {
		var doc DocumentResponse
		json.Unmarshal(*hit.Source, &doc)
		docs = append(docs, doc)
	}
	res.Documents = docs
	c.JSON(http.StatusOK, res)
}

func main() {
	var err error
	elasticClient, err = elastic.NewClient(
		elastic.SetURL("http://elasticsearch:9200"),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Println(err)
	}
	go func() {
		time.Sleep(3 * time.Second)
		for {
			elasticClient, err = elastic.NewClient(
				elastic.SetURL("http://elasticsearch:9200"),
				elastic.SetSniff(false),
			)
			if err != nil {
				log.Println(err)
				time.Sleep(3 * time.Second)
			} else {
				break
			}
		}
	}()
	r := gin.Default()
	r.POST("/documents", createDocumentsEndpoint)
	r.GET("/search", searchEndpoint)
	r.GET("/redis", redisH)
	r.POST("/couchbaseInsert", couchInsert)
	r.GET("/couchbase", couchGet)
	r.GET("/", handler)
	if err = r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
