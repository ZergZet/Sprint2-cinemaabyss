package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type MovieEvent struct {
	MovieID     int       `json:"movie_id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Genres      []string  `json:"genres"`
	Rating      float64   `json:"rating"`
	Action      string    `json:"action"`
	UserID      int       `json:"user_id"`
	Timestamp   time.Time `json:"timestamp"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID int       `json:"payment_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type EventProducer interface {
	Produce(ctx context.Context, topic string, event interface{}) error
	Close() error
}

type EventConsumer interface {
	StartConsume(ctx context.Context, topic string, wg *sync.WaitGroup)
}

// Реализация Producer
type KafkaProducer struct {
	writer *kafka.Writer
}

// конструктор продюсера Kafka
func NewKafkaProducer(brokers []string) *KafkaProducer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	return &KafkaProducer{writer: writer}
}

// отправка события в Kafka
func (p *KafkaProducer) Produce(ctx context.Context, topic string, event interface{}) error {
	eventBytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: eventBytes,
	})
}

// закрытие продюсера
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// реализация Consumer
type KafkaConsumer struct {
	brokers []string
	groupID string
}

func NewKafkaConsumer(brokers []string, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		brokers: brokers,
		groupID: groupID,
	}
}

func (c *KafkaConsumer) StartConsume(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  c.brokers,
		Topic:    topic,
		GroupID:  c.groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("Consumer started for topic %s", topic)

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from topic %s: %v", topic, err)
			break
		}
		log.Printf("[CONSUMER] Received message from topic %s at offset %d: %s = %s\n",
			m.Topic, m.Offset, string(m.Key), string(m.Value))
	}
}

// Обработчик HTTP событий
type EventHandler struct {
	producer EventProducer
}

// конструктор обработчика
func NewEventHandler(producer EventProducer) *EventHandler {
	return &EventHandler{producer: producer}
}

// создание HTTP-обработчика для конкретного топика
func (h *EventHandler) MakeHandler(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var eventData interface{}
		switch topic {
		case "movie-events":
			eventData = &MovieEvent{}
		case "user-events":
			eventData = &UserEvent{}
		case "payment-events":
			eventData = &PaymentEvent{}
		default:
			http.Error(w, "Unknown event type", http.StatusBadRequest)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(eventData); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := h.producer.Produce(r.Context(), topic, eventData)
		if err != nil {
			log.Printf("Failed to write message to Kafka: %v", err)
			http.Error(w, "Failed to write message to Kafka", http.StatusInternalServerError)
			return
		}

		eventBytes, _ := json.Marshal(eventData)
		log.Printf("Successfully produced message to topic %s: %s", topic, string(eventBytes))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}
}

// проверка состояния сервиса
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

type EventService struct {
	producer     EventProducer
	consumer     EventConsumer
	httpHandlers map[string]http.HandlerFunc
	port         string
}

// конструктор сервиса событий
func NewEventService(producer EventProducer, consumer EventConsumer, port string) *EventService {
	s := &EventService{
		producer:     producer,
		consumer:     consumer,
		httpHandlers: make(map[string]http.HandlerFunc),
		port:         port,
	}
	// cоздание обработчиков топиков
	handler := NewEventHandler(producer)
	s.httpHandlers["/api/events/movie"] = handler.MakeHandler("movie-events")
	s.httpHandlers["/api/events/user"] = handler.MakeHandler("user-events")
	s.httpHandlers["/api/events/payment"] = handler.MakeHandler("payment-events")
	s.httpHandlers["/api/events/health"] = handleHealth
	return s
}

// запуск сервиса
func (s *EventService) Start(ctx context.Context) error {
	// запуск консьюмеров каждого топика
	topics := []string{"movie-events", "user-events", "payment-events"}
	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go s.consumer.StartConsume(ctx, topic, &wg)
	}

	// настройка HTTP маршрутов
	mux := http.NewServeMux()
	for path, handler := range s.httpHandlers {
		mux.HandleFunc(path, handler)
	}

	// запуск HTTP сервера
	log.Printf("Events service starting on port %s", s.port)
	return http.ListenAndServe(":"+s.port, mux)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokers, ",")
	port := getEnv("PORT", "8082")

	producer := NewKafkaProducer(brokers)
	defer producer.Close()

	consumer := NewKafkaConsumer(brokers, "cinemaabyss-events-consumer-group")

	service := NewEventService(producer, consumer, port)

	ctx := context.Background()
	if err := service.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
