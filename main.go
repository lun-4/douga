package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	gonanoid "github.com/matoous/go-nanoid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
)

type Config struct {
	ServerURL  string
	Port       string
	DBPath     string
	AppviewURL string
	PLCUrl     string
}

type DIDDocument struct {
	Context []string  `json:"@context"`
	ID      string    `json:"id"`
	Service []Service `json:"service"`
}

type Service struct {
	ID              string `json:"id"`
	Type            string `json:"type"`
	ServiceEndpoint string `json:"serviceEndpoint"`
}

func newDIDDocument(serverURL string) DIDDocument {
	return DIDDocument{
		Context: []string{"https://www.w3.org/ns/did/v1"},
		ID:      fmt.Sprintf("did:web:%s", serverURL),
		Service: []Service{
			{
				ID:              "#bsky_chat",
				Type:            "BskyChatService",
				ServiceEndpoint: fmt.Sprintf("https://%s", serverURL),
			},
		},
	}
}

type Storage struct {
	db         *sql.DB
	plcUrl     string
	appviewUrl string
}

type User struct {
	pdsUrl string
}

func (st Storage) fetchUser(userDID string) (*User, error) {
	res, err := http.Get(fmt.Sprintf("%s/%s", st.plcUrl, userDID))
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching user %s failed: %s", userDID, res.Status)
	}

	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading user %s failed: %s", userDID, err)
	}

	var plcData DIDDocument
	err = json.Unmarshal(body, &plcData)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling user %s failed: %s", userDID, err)
	}
	var pdsUrl string
	for _, service := range plcData.Service {
		if service.ID == "#atproto_pds" && service.Type == "AtprotoPersonalDataServer" {
			pdsUrl = service.ServiceEndpoint
		}
	}

	u := User{
		pdsUrl: pdsUrl,
	}
	return &u, nil
}

type State struct {
	storage *Storage
	jobs    sync.Map
}

func (s *State) getUploadLimits(c *gin.Context) {
	// userDID := c.GetString("user_did")
	out := bsky.VideoGetUploadLimits_Output{
		CanUpload:            true,
		RemainingDailyBytes:  lo.ToPtr(int64(10000000)),
		RemainingDailyVideos: lo.ToPtr(int64(2000)),
	}

	c.JSON(200, out)
}

func (s *State) update(job Job) {
	log.Printf("State update: %s %s %d %s %v %v", job.ID, job.contentType, job.progress, job.state, job.err, job.blob)
	s.jobs.Store(job.ID, job)
}
func (s *State) process(job Job, body []byte) {
	log.Printf("Processing job: %s", job.ID)
	err := s.processJob(job, body)
	if err != nil {
		log.Printf("Error processing job %s: %s", job.ID, err)
		job.err = err
		job.state = "JOB_STATE_FAILED"
		s.update(job)
		return
	}
}
func (s *State) processJob(job Job, body []byte) error {
	u, err := s.storage.fetchUser(job.userDID)
	if err != nil {
		return fmt.Errorf("failed to fetch user: %v", err)
	}
	if u.pdsUrl == "" {
		return fmt.Errorf("user %s has no PDS", job.userDID)
	}
	{
		job.progress = 10
		s.update(job)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/xrpc/com.atproto.repo.uploadBlob", u.pdsUrl), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create req: %s", err)
	}
	req.Header.Set("authorization", job.token)
	req.Header.Set("content-type", job.contentType)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("upload error %s", err)
	}
	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("upload error %s, %s", res.Status, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("upload error %s, %s", res.Status, string(resBody))
	}
	out := atproto.RepoUploadBlob_Output{}
	err = json.Unmarshal(resBody, &out)
	if err != nil {
		return fmt.Errorf("failed to unmarshall upload result: %w", err)
	}
	{
		log.Printf("uploaded! %s", out.Blob.Ref)
		job.progress = 100
		job.state = "JOB_STATE_COMPLETED"
		job.blob = out.Blob
		s.update(job)
	}
	return nil
}

func (s *State) uploadVideo(c *gin.Context) {
	// userDID := c.GetString("user_did")
	userDID := c.Query("did")
	jobID := gonanoid.MustGenerate("abcdefghimnopqrstuvwxyz1234567890", 10)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	job := Job{
		ID:          jobID,
		userDID:     userDID,
		state:       "processing",
		progress:    1,
		token:       c.GetHeader("authorization"),
		contentType: c.GetHeader("content-type"),
	}
	s.jobs.Store(jobID, job)
	go s.process(job, body)
	c.JSON(200, job.ToBsky())
}

type Job struct {
	ID          string
	userDID     string
	state       string
	progress    int64
	err         error
	blob        *util.LexBlob
	token       string
	contentType string
}

func (j Job) ToBsky() *bsky.VideoDefs_JobStatus {
	switch j.state {
	case "processing":
		return &bsky.VideoDefs_JobStatus{
			JobId:    j.ID,
			Did:      j.userDID,
			State:    j.state,
			Progress: lo.ToPtr(int64(j.progress)),
			Message:  lo.ToPtr("processing..."),
		}
	case "JOB_STATE_COMPLETED":
		return &bsky.VideoDefs_JobStatus{
			JobId:    j.ID,
			Did:      j.userDID,
			State:    j.state,
			Progress: lo.ToPtr(int64(j.progress)),
			Blob:     j.blob,
			Message:  lo.ToPtr("uploaded!"),
		}
	case "JOB_STATE_FAILED":
		return &bsky.VideoDefs_JobStatus{
			JobId:    j.ID,
			Did:      j.userDID,
			State:    j.state,
			Progress: lo.ToPtr(int64(j.progress)),
			Error:    lo.ToPtr(j.err.Error()),
			Message:  lo.ToPtr(j.err.Error()),
		}
	default:
		panic("invalid state " + j.state)
	}
}

func (s *State) getJobStatus(c *gin.Context) {
	jobID := c.Query("jobId")
	jobA, ok := s.jobs.Load(jobID)
	if !ok {
		c.AbortWithError(http.StatusBadRequest, errors.New("invalid job id"))
		return
	}
	job := jobA.(Job)
	out := bsky.VideoGetJobStatus_Output{
		JobStatus: job.ToBsky(),
	}

	c.JSON(200, out)
}

func requestDebugMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get the raw request body
		var bodyBytes []byte
		if c.Request.Body != nil && c.Request.Header.Get("content-type") == "application/json" {
			bodyBytes, _ = io.ReadAll(c.Request.Body)
			// Restore the body for later middleware/handlers
			c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		} else {
			bodyBytes = []byte("<stripped data>")
		}

		// Print request details
		fmt.Printf("\n==== Incoming Request ====\n")
		fmt.Printf("Method: %s\n", c.Request.Method)
		fmt.Printf("URL: %s\n", c.Request.URL.String())

		// Print headers
		fmt.Println("\nHeaders:")
		for name, values := range c.Request.Header {
			fmt.Printf("%s: %s\n", name, strings.Join(values, ", "))
		}

		// Print query parameters
		fmt.Println("\nQuery Parameters:")
		for key, values := range c.Request.URL.Query() {
			fmt.Printf("%s: %s\n", key, strings.Join(values, ", "))
		}

		// Print body if exists
		if len(bodyBytes) > 0 {
			fmt.Println("\nBody:")
			// Try to pretty print JSON
			var prettyJSON bytes.Buffer
			if err := json.Indent(&prettyJSON, bodyBytes, "", "  "); err == nil {
				fmt.Println(prettyJSON.String())
			} else {
				// If not JSON, print raw body
				fmt.Println(string(bodyBytes))
			}
		}

		fmt.Println("\n========================")

		c.Next()
	}
}

func main() {
	// Initialize configuration
	config := Config{
		ServerURL:  getEnvOrDefault("SERVER_URL", "chat.example.net"),
		Port:       getEnvOrDefault("PORT", "3000"),
		DBPath:     getEnvOrDefault("DB_PATH", "data.db"),
		AppviewURL: getEnvOrDefault("APPVIEW_URL", ""),
		PLCUrl:     getEnvOrDefault("ATPROTO_PLC_URL", ""),
	}

	db, err := sql.Open("sqlite3", config.DBPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
	PRAGMA journal_mode=WAL;
	PRAGMA busy_timeout = 5000;
	PRAGMA synchronous = NORMAL;
	PRAGMA cache_size = 1000000000;
	PRAGMA foreign_keys = true;
	PRAGMA temp_store = memory;

	CREATE TABLE IF NOT EXISTS users (
		did text primary key,
		handle text
	) STRICT;
	`)
	if err != nil {
		log.Fatalf("Error creating tables: %v", err)
	}

	storage := Storage{db: db, appviewUrl: config.AppviewURL, plcUrl: config.PLCUrl}
	state := State{storage: &storage}

	// Create Gin router
	r := gin.New()

	// Middleware
	r.Use(gin.Recovery())
	r.Use(gin.Logger())
	r.Use(requestDebugMiddleware())

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{config.AppviewURL},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH"},
		AllowHeaders:     []string{"Origin", "Authorization", "atproto-accept-labelers", "content-type", "content-length"},
		ExposeHeaders:    []string{"Content-Length", "Content-Type"},
		AllowCredentials: true,
		AllowOriginFunc: func(origin string) bool {
			return origin == config.AppviewURL
		},
		MaxAge: 12 * time.Hour,
	}))

	serviceWebDID := "did:web:" + config.ServerURL
	auther, err := NewAuth(
		100_000,
		time.Hour*12,
		5,
		serviceWebDID,
	)
	if err != nil {
		log.Fatalf("Failed to create Auth: %v", err)
	}
	authGroup := r.Group("/")
	authGroup.Use(auther.AuthenticateGinRequestViaJWT)
	authGroup.GET("/xrpc/app.bsky.video.getUploadLimits", state.getUploadLimits)
	authGroup.GET("/xrpc/app.bsky.video.getJobStatus", state.getJobStatus)
	r.POST("/xrpc/app.bsky.video.uploadVideo", state.uploadVideo)

	// TODO implement
	r.GET("/watch/{did}/{cid}/playlist.m3u8", state.getVideo)
	r.GET("/watch/{did}/{cid}/thumbnail.jpg", state.getThumbnail)

	r.GET("/", func(c *gin.Context) {
		c.String(200, "https://github.com/lun-4/douga -- a reimplementation of video.bsky.app for the bit")
	})
	r.GET("/.well-known/did.json", func(c *gin.Context) {
		didDoc := newDIDDocument(config.ServerURL)
		c.JSON(200, didDoc)
	})

	// Start server
	addr := ":" + config.Port
	fmt.Printf("Server starting on %s\n", addr)
	r.Run(addr)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return defaultValue
}
