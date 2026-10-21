package mercure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

const (
	lastEventIDKey = "lastEventID"
	publishScript  = `
		redis.call("SET", KEYS[1], ARGV[1])
		redis.call("PUBLISH", ARGV[2], ARGV[3])
		return true
	`
)

type RedisTransport struct {
	sync.RWMutex
	logger        *slog.Logger
	client        *redis.Client
	subscribers   *SubscriberList
	closed        chan any
	publishScript *redis.Script
	closedOnce    sync.Once
	redisChannel  string
}

func NewRedisTransport(
	subscriberList *SubscriberList,
	logger *slog.Logger,
	address string,
	username string,
	password string,
	redisChannel string,
) (*RedisTransport, error) {
	client := redis.NewClient(&redis.Options{
		Username: username,
		Password: password,
		Addr:     address,
	})

	if pong := client.Ping(context.Background()); pong.String() != "ping: PONG" {
		return nil, fmt.Errorf("failed to connect to Redis: %w", pong.Err())
	}

	return NewRedisTransportInstance(subscriberList, logger, client, redisChannel)
}

func NewRedisTransportInstance(
	subscriberList *SubscriberList,
	logger *slog.Logger,
	client *redis.Client,
	redisChannel string,
) (*RedisTransport, error) {
	subscriber := client.PSubscribe(context.Background(), redisChannel)

	subscribeCtx, subscribeCancel := context.WithCancel(context.Background())

	transport := &RedisTransport{
		logger:        logger,
		client:        client,
		subscribers:   subscriberList,
		publishScript: redis.NewScript(publishScript),
		closed:        make(chan any),
		redisChannel:  redisChannel,
	}

	go func() {
		select {
		case <-transport.closed:
			if err := subscriber.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
				logger.Error("Failed to close Redis subscriber", "error", err)
			}
			<-subscribeCtx.Done()
			if err := client.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
				logger.Error("Failed to close Redis client", "error", err)
			}
		case <-subscribeCtx.Done():
		}
	}()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		transport.subscribe(subscribeCtx, subscribeCancel, subscriber)
	}()

	return transport, nil
}

func (u Update) MarshalBinary() ([]byte, error) {
	bytes, err := json.Marshal(u)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal: %w", err)
	}

	return bytes, nil
}

func (t *RedisTransport) Dispatch(ctx context.Context, update *Update) error {
	select {
	case <-t.closed:

		return ErrClosedTransport
	default:
	}

	update.AssignUUID()

	keys := []string{lastEventIDKey}
	arguments := []interface{}{update.ID, t.redisChannel, update}
	_, err := t.publishScript.Run(ctx, t.client, keys, arguments...).Result()
	if err != nil {
		return fmt.Errorf("redis failed to publish: %w", err)
	}

	return nil
}

func (t *RedisTransport) AddSubscriber(ctx context.Context, s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.Lock()
	t.subscribers.Add(s)
	t.Unlock()
	if s.RequestLastEventIDSet {
		s.HistoryDispatched(EarliestLastEventID)
	}
	s.Ready(ctx)

	return nil
}

func (t *RedisTransport) RemoveSubscriber(ctx context.Context, s *LocalSubscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}
	t.Lock()
	defer t.Unlock()
	t.subscribers.Remove(s)

	return nil
}

func (t *RedisTransport) GetSubscribers(ctx context.Context) (string, []*Subscriber, error) {
	select {
	case <-t.closed:
		return "", nil, ErrClosedTransport
	default:
	}
	t.RLock()
	defer t.RUnlock()
	lastEventID, err := t.client.Get(ctx, lastEventIDKey).Result()
	if err != nil {
		return "", nil, fmt.Errorf("redis failed to get last event id: %w", err)
	}

	return lastEventID, getSubscribers(t.subscribers), nil
}

func (t *RedisTransport) Close(ctx context.Context) (err error) {
	t.closedOnce.Do(func() {
		t.Lock()
		defer t.Unlock()
		t.subscribers.Walk(0, func(s *LocalSubscriber) bool {
			s.Disconnect()

			return true
		})
		close(t.closed)
	})

	return nil
}

func (t *RedisTransport) subscribe(ctx context.Context, cancel context.CancelFunc, subscriber *redis.PubSub) {
	for {
		message, err := subscriber.ReceiveMessage(ctx)
		if err != nil {
			if errors.Is(err, redis.ErrClosed) {
				cancel()

				return
			}

			t.logger.Error("Failed to receive Redis message", "error", err)

			continue
		}
		var update Update
		if err := json.Unmarshal([]byte(message.Payload), &update); err != nil {
			t.logger.Error("Failed to unmarshal update from Redis", "error", err)

			continue
		}
		topics := make([]string, len(update.Topics))
		copy(topics, update.Topics)
		t.Lock()
		for _, subscriber := range t.subscribers.MatchAny(&update) {
			update.Topics = topics
			subscriber.Dispatch(ctx, &update, false)
		}
		t.Unlock()
	}
}

var (
	_ Transport            = (*RedisTransport)(nil)
	_ TransportSubscribers = (*RedisTransport)(nil)
)