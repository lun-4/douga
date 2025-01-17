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
	"os/exec"
	"path/filepath"
	"slices"
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
	ServerHostname string
	Port           string
	DBPath         string
	AppviewURL     string
	FrontendURL    string
	PLCUrl         string
	AllowedDIDs    string
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
	storage     *Storage
	jobs        sync.Map
	cm          *ConversionManager
	allowedDIDs []string
}

func (s *State) getUploadLimits(c *gin.Context) {
	userDID := c.GetString("user_did")
	out := bsky.VideoGetUploadLimits_Output{
		CanUpload:            true,
		RemainingDailyBytes:  lo.ToPtr(int64(10000000)),
		RemainingDailyVideos: lo.ToPtr(int64(2000)),
	}
	if len(s.allowedDIDs) > 0 && !slices.Contains(s.allowedDIDs, userDID) {
		out.CanUpload = false
		out.RemainingDailyBytes = lo.ToPtr(int64(0))
		out.RemainingDailyVideos = lo.ToPtr(int64(0))
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
	if len(s.allowedDIDs) > 0 && !slices.Contains(s.allowedDIDs, userDID) {
		c.AbortWithError(http.StatusForbidden, fmt.Errorf("DID not allowed"))
		return
	}
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

type ConversionManager struct {
	mu            sync.RWMutex
	conversions   sync.Map
	thumbnails    sync.Map
	cleanupTicker *time.Ticker
	config        Config
}

type Conversion struct {
	OutputDir    string
	LastAccessed time.Time
	Converting   bool
	Error        error
}

type Thumbnail struct {
	Path         string
	LastAccessed time.Time
	Generating   bool
	Error        error
}

func NewConversionManager(config Config) *ConversionManager {
	cm := &ConversionManager{
		conversions:   sync.Map{},
		cleanupTicker: time.NewTicker(5 * time.Minute),
		config:        config,
	}
	go cm.cleanupRoutine()
	return cm
}

func (cm *ConversionManager) cleanupRoutine() {
	for range cm.cleanupTicker.C {
		cm.mu.Lock()
		now := time.Now()

		// Cleanup conversions
		keysToRemove := make([]string, 0)
		cm.conversions.Range(func(keyA any, convA any) bool {
			key := keyA.(string)
			conv := convA.(*Conversion)

			if now.Sub(conv.LastAccessed) > 30*time.Minute {
				keysToRemove = append(keysToRemove, key)
				os.RemoveAll(conv.OutputDir)
			}
			return true
		})
		for _, k := range keysToRemove {
			cm.conversions.Delete(k)
		}

		// Cleanup thumbnails
		thumbsToRemove := make([]string, 0)
		cm.thumbnails.Range(func(keyA any, thumbA any) bool {
			key := keyA.(string)
			thumb := thumbA.(*Thumbnail)

			if now.Sub(thumb.LastAccessed) > 30*time.Minute {
				thumbsToRemove = append(thumbsToRemove, key)
				os.RemoveAll(filepath.Dir(thumb.Path))
			}
			return true
		})
		for _, k := range thumbsToRemove {
			cm.thumbnails.Delete(k)
		}

		cm.mu.Unlock()
	}
}

// Add this method to ConversionManager
func (cm *ConversionManager) getOrCreateThumbnail(did, cid string) (*Thumbnail, error) {
	key := fmt.Sprintf("thumb_%s_%s", did, cid)
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if thumbA, exists := cm.thumbnails.Load(key); exists {
		thumb := thumbA.(*Thumbnail)
		thumb.LastAccessed = time.Now()
		return thumb, nil
	}

	// Create new temporary directory for thumbnail
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("thumb_%s_%s_*", did, cid))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory for thumbnail: %w", err)
	}

	thumb := &Thumbnail{
		Path:         filepath.Join(tmpDir, "thumbnail.jpg"),
		LastAccessed: time.Now(),
		Generating:   false,
	}
	cm.thumbnails.Store(key, thumb)
	return thumb, nil
}

// Add thumbnail generation method
func (cm *ConversionManager) generateThumbnail(did, cid string, thumb *Thumbnail) error {
	cm.mu.Lock()
	if thumb.Generating {
		cm.mu.Unlock()
		return nil // Generation already in progress
	}
	thumb.Generating = true
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		thumb.Generating = false
		cm.mu.Unlock()
	}()

	sourceURL := fmt.Sprintf("%s/blob/%s/%s", cm.config.AppviewURL, did, cid)

	// Download blob to temporary storage
	tmpFile, err := cm.downloadBlob(sourceURL)
	if err != nil {
		thumb.Error = fmt.Errorf("failed to download blob for thumbnail: %w", err)
		return thumb.Error
	}
	defer os.Remove(tmpFile)

	// Generate thumbnail using ffmpeg
	// This command will extract a frame at 1 second mark and create a thumbnail
	cmd := exec.Command(
		"ffmpeg",
		"-i", tmpFile,
		"-ss", "00:00:01.000",
		"-vframes", "1",
		"-vf", "scale=480:-1",
		"-y",
		thumb.Path,
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		thumb.Error = fmt.Errorf("ffmpeg thumbnail error: %v, output: %s", err, output)
		return thumb.Error
	}

	return nil
}

func (cm *ConversionManager) getOrCreateConversion(did, cid string) (*Conversion, error) {
	key := fmt.Sprintf("%s/%s", did, cid)
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if convA, exists := cm.conversions.Load(key); exists {
		conv := convA.(*Conversion)
		conv.LastAccessed = time.Now()
		return conv, nil
	}

	// Create new temporary directory
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("hls_%s_%s_*", did, cid))
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	conv := &Conversion{
		OutputDir:    tmpDir,
		LastAccessed: time.Now(),
		Converting:   false,
	}
	cm.conversions.Store(key, conv)
	return conv, nil
}

func (cm *ConversionManager) downloadBlob(sourceURL string) (string, error) {
	// Create temporary file for the downloaded blob
	tmpFile, err := os.CreateTemp("", "blob_*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tmpFile.Close()

	// Download the blob
	resp, err := http.Get(sourceURL)
	if err != nil {
		return "", fmt.Errorf("failed to download blob: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download blob: HTTP %d", resp.StatusCode)
	}

	// Copy the blob to temporary file
	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save blob: %w", err)
	}

	return tmpFile.Name(), nil
}

func (cm *ConversionManager) convertToHLS(did, cid string, conv *Conversion) error {
	cm.mu.Lock()
	if conv.Converting {
		cm.mu.Unlock()
		return nil // Conversion already in progress
	}
	conv.Converting = true
	cm.mu.Unlock()

	defer func() {
		cm.mu.Lock()
		conv.Converting = false
		cm.mu.Unlock()
	}()

	sourceURL := fmt.Sprintf("%s/blob/%s/%s", cm.config.AppviewURL, did, cid)

	// Download blob to temporary storage
	tmpFile, err := cm.downloadBlob(sourceURL)
	if err != nil {
		conv.Error = fmt.Errorf("failed to download blob: %w", err)
		return conv.Error
	}
	// Clean up the temporary file when done
	defer os.Remove(tmpFile)

	log.Printf("Converted %s to HLS", cid)
	log.Printf("temp stored at: %s", tmpFile)

	cmd := exec.Command(
		"ffmpeg",
		"-i", tmpFile,
		"-profile:v", "baseline",
		"-level", "3.0",
		"-start_number", "0",
		"-hls_time", "10", // TODO segment length configurable?
		"-hls_list_size", "0",
		"-f", "hls",
		"-hls_segment_filename", filepath.Join(conv.OutputDir, "segment%d.ts"),
		filepath.Join(conv.OutputDir, "playlist.m3u8"),
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		conv.Error = fmt.Errorf("ffmpeg error: %v, output: %s", err, output)
		return conv.Error
	}

	return nil
}

func (s *State) getVideoOrThumbnail(c *gin.Context) {
	did := c.Param("did")
	cid := c.Param("cid")
	if did == "" {
		c.AbortWithError(http.StatusBadRequest, errors.New("did is missing"))
		return
	}
	if cid == "" {
		c.AbortWithError(http.StatusBadRequest, errors.New("cid is missing"))
		return
	}

	filename := filepath.Base(c.Param("filepath"))
	if filename == "thumbnail.jpg" {
		s.getThumbnail(c)
		return
	}

	// Validate that we're only serving allowed files
	if filename != "playlist.m3u8" && filepath.Ext(filename) != ".ts" {
		c.AbortWithError(http.StatusBadRequest, errors.New("invalid file request"))
		return
	}

	conv, err := s.cm.getOrCreateConversion(did, cid)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	// Check if we need to start conversion
	if _, err := os.Stat(filepath.Join(conv.OutputDir, "playlist.m3u8")); os.IsNotExist(err) {
		if err := s.cm.convertToHLS(did, cid, conv); err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// Set appropriate headers
	if filepath.Ext(filename) == ".m3u8" {
		c.Header("Content-Type", "application/vnd.apple.mpegurl")
	} else {
		c.Header("Content-Type", "video/mp2t")
	}

	c.Header("Access-Control-Allow-Origin", "*")

	// Serve the file
	c.File(filepath.Join(conv.OutputDir, filename))
}

// Add getThumbnail handler to State
func (s *State) getThumbnail(c *gin.Context) {
	did := c.Param("did")
	cid := c.Param("cid")

	if did == "" {
		c.AbortWithError(http.StatusBadRequest, errors.New("did is missing"))
		return
	}
	if cid == "" {
		c.AbortWithError(http.StatusBadRequest, errors.New("cid is missing"))
		return
	}

	thumb, err := s.cm.getOrCreateThumbnail(did, cid)
	if err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	// Check if we need to generate thumbnail
	if _, err := os.Stat(thumb.Path); os.IsNotExist(err) {
		if err := s.cm.generateThumbnail(did, cid, thumb); err != nil {
			c.AbortWithError(http.StatusInternalServerError, err)
			return
		}
	}

	// Set appropriate headers
	c.Header("Content-Type", "image/jpeg")
	c.Header("Cache-Control", "public, max-age=31536000")
	c.Header("Access-Control-Allow-Origin", "*")

	// Serve the thumbnail
	c.File(thumb.Path)
}

func main() {
	// Initialize configuration
	config := Config{
		ServerHostname: getEnvOrDefault("SERVER_HOSTNAME", "chat.example.net"),
		Port:           getEnvOrDefault("PORT", "3000"),
		DBPath:         getEnvOrDefault("DB_PATH", "data.db"),
		AppviewURL:     getEnvOrDefault("APPVIEW_URL", ""),
		FrontendURL:    getEnvOrDefault("FRONTEND_URL", ""),
		PLCUrl:         getEnvOrDefault("ATPROTO_PLC_URL", ""),
		AllowedDIDs:    getEnvOrDefault("ALLOWED_DIDS", ""),
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

	allowedDIDs := make([]string, 0)
	if config.AllowedDIDs != "" {
		for _, did := range strings.Split(config.AllowedDIDs, ",") {
			allowedDIDs = append(allowedDIDs, did)
		}
	}

	storage := Storage{db: db, appviewUrl: config.AppviewURL, plcUrl: config.PLCUrl}
	cm := NewConversionManager(config)
	state := State{storage: &storage, cm: cm, allowedDIDs: allowedDIDs}

	// Create Gin router
	r := gin.New()

	// Middleware
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{config.AppviewURL, config.FrontendURL},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH"},
		AllowHeaders:     []string{"Origin", "Authorization", "atproto-accept-labelers", "content-type", "content-length"},
		ExposeHeaders:    []string{"Content-Length", "Content-Type"},
		AllowCredentials: true,
		AllowOriginFunc: func(origin string) bool {
			return origin == config.AppviewURL || origin == config.FrontendURL
		},
		MaxAge: 12 * time.Hour,
	}))

	serviceWebDID := "did:web:" + config.ServerHostname
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
	r.GET("/watch/:did/:cid/*filepath", state.getVideoOrThumbnail)

	r.GET("/", func(c *gin.Context) {
		c.String(200, "https://github.com/lun-4/douga -- a reimplementation of video.bsky.app for the bit")
	})
	r.GET("/.well-known/did.json", func(c *gin.Context) {
		didDoc := newDIDDocument(config.ServerHostname)
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
