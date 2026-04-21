package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// настройки приложения, полученные из переменных окружения
type Config struct {
	Port                    string
	MonolithURL             string
	MoviesServiceURL        string
	EventsServiceURL        string
	GradualMigrationEnabled bool
	MoviesMigrationPercent  int
}

// загрузка конфигурации из окружения
func LoadConfig() *Config {
	rand.Seed(time.Now().UnixNano())

	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	gradualMigrationEnabled := getEnv("GRADUAL_MIGRATION", "false") == "true"
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")

	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT value, defaulting to 0. Error: %v", err)
		migrationPercent = 0
	}

	return &Config{
		Port:                    port,
		MonolithURL:             monolithURL,
		MoviesServiceURL:        moviesServiceURL,
		EventsServiceURL:        eventsServiceURL,
		GradualMigrationEnabled: gradualMigrationEnabled,
		MoviesMigrationPercent:  migrationPercent,
	}
}

// объединяет прокси-серверы и логику маршрутизации
type StranglerFigProxy struct {
	config   *Config
	monolith *httputil.ReverseProxy
	movies   *httputil.ReverseProxy
	events   *httputil.ReverseProxy
}

// создание нового прокси на основе конфигурации.
func NewStranglerFigProxy(cfg *Config) (*StranglerFigProxy, error) {
	monoURL, err := url.Parse(cfg.MonolithURL)
	if err != nil {
		return nil, err
	}
	movURL, err := url.Parse(cfg.MoviesServiceURL)
	if err != nil {
		return nil, err
	}
	evtURL, err := url.Parse(cfg.EventsServiceURL)
	if err != nil {
		return nil, err
	}

	return &StranglerFigProxy{
		config:   cfg,
		monolith: httputil.NewSingleHostReverseProxy(monoURL),
		movies:   httputil.NewSingleHostReverseProxy(movURL),
		events:   httputil.NewSingleHostReverseProxy(evtURL),
	}, nil
}

// реализация http.Handler с основной логикой маршрутизации
func (p *StranglerFigProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Incoming request: %s %s", r.Method, r.URL.Path)

	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if p.config.GradualMigrationEnabled && rand.Intn(100) < p.config.MoviesMigrationPercent {
			log.Printf("Routing to movies-service (migration)")
			p.movies.ServeHTTP(w, r)
		} else {
			log.Printf("Routing to monolith")
			p.monolith.ServeHTTP(w, r)
		}
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		log.Printf("Routing to events-service")
		p.events.ServeHTTP(w, r)
	default:
		log.Printf("Routing to monolith (default)")
		p.monolith.ServeHTTP(w, r)
	}
}

// обработка запросов к /health
func (p *StranglerFigProxy) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Strangler Fig Proxy is healthy"))
}

// запуск http-сервера
func (p *StranglerFigProxy) Run() error {
	http.Handle("/", p)
	http.HandleFunc("/health", p.HealthHandler)

	log.Printf("Strangler Fig Proxy started on port %s", p.config.Port)
	log.Printf("Monolith URL: %s", p.config.MonolithURL)
	log.Printf("Movies Service URL: %s", p.config.MoviesServiceURL)
	log.Printf("Gradual migration enabled: %v", p.config.GradualMigrationEnabled)
	log.Printf("Movies migration percentage: %d%%", p.config.MoviesMigrationPercent)

	return http.ListenAndServe(":"+p.config.Port, nil)
}

// чтение переменных окружения
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	cfg := LoadConfig()
	proxy, err := NewStranglerFigProxy(cfg)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}
	if err := proxy.Run(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
