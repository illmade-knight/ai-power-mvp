package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"time"

	"github.com/rs/zerolog"
)

// RedisConfig holds configuration for the Redis client.
type RedisConfig struct {
	Addr     string        // e.g., "localhost:6379"
	Password string        // Leave empty if no password
	DB       int           // e.g., 0
	CacheTTL time.Duration // Time-to-live for cache entries, e.g., 5 * time.Minute
}

// RedisDeviceMetadataFetcher implements DeviceMetadataFetcher using a Redis cache
// that falls back to another DeviceMetadataFetcher (like Firestore) on a cache miss.
type RedisDeviceMetadataFetcher struct {
	redisClient     *redis.Client
	fallbackFetcher DeviceMetadataFetcher // The source of truth (e.g., Firestore fetcher)
	logger          zerolog.Logger
	ttl             time.Duration
	// Use a shared context for Redis operations.
	ctx context.Context
}

// deviceMetadataCache a helper struct for JSON serialization in Redis.
// This ensures that the data stored in Redis has a consistent structure.
type deviceMetadataCache struct {
	ClientID   string `json:"clientID"`
	LocationID string `json:"locationID"`
	Category   string `json:"category"`
}

// NewRedisDeviceMetadataFetcher creates a new caching fetcher.
// It takes a Redis configuration and a fallback fetcher (e.g., the Firestore one).
func NewRedisDeviceMetadataFetcher(
	ctx context.Context,
	cfg *RedisConfig,
	fallback DeviceMetadataFetcher,
	logger zerolog.Logger,
) (*RedisDeviceMetadataFetcher, error) {
	if fallback == nil {
		return nil, errors.New("fallback fetcher cannot be nil")
	}

	// Create a new Redis client.
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Ping the Redis server to ensure a connection is established.
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	logger.Info().Str("redis_address", cfg.Addr).Msg("Successfully connected to Redis for device cache")

	return &RedisDeviceMetadataFetcher{
		redisClient:     rdb,
		fallbackFetcher: fallback,
		logger:          logger,
		ttl:             cfg.CacheTTL,
		ctx:             ctx,
	}, nil
}

// Fetch retrieves device metadata, checking Redis first.
// If the data is not in the cache (a "cache miss"), it calls the fallback
// fetcher, stores the result in Redis for future requests, and then returns the data.
func (f *RedisDeviceMetadataFetcher) Fetch(deviceEUI string) (clientID, locationID, category string, err error) {
	// 1. Try to get the device from the Redis cache.
	cachedData, err := f.redisClient.Get(f.ctx, deviceEUI).Result()
	if err == nil {
		// Cache Hit!
		f.logger.Debug().Str("device_eui", deviceEUI).Msg("Cache hit: Found device metadata in Redis")
		var metadata deviceMetadataCache
		if jsonErr := json.Unmarshal([]byte(cachedData), &metadata); jsonErr != nil {
			f.logger.Error().Err(jsonErr).Str("device_eui", deviceEUI).Msg("Failed to unmarshal cached device data from Redis")
			// Treat as a cache miss if data is corrupted.
		} else {
			// Successfully retrieved and unmarshaled from cache.
			return metadata.ClientID, metadata.LocationID, metadata.Category, nil
		}
	}

	if !errors.Is(err, redis.Nil) {
		// An actual error occurred with Redis, not just a cache miss.
		f.logger.Error().Err(err).Str("device_eui", deviceEUI).Msg("Error fetching from Redis cache")
	} else {
		f.logger.Debug().Str("device_eui", deviceEUI).Msg("Cache miss: Device metadata not found in Redis")
	}

	// 2. Cache Miss: Fetch from the fallback (Firestore).
	clientID, locationID, category, err = f.fallbackFetcher(deviceEUI)
	if err != nil {
		// If the fallback couldn't find it or failed, return the error.
		// Do not cache negative results like "not found".
		return "", "", "", err
	}

	// 3. Cache the result in Redis for next time.
	metadataToCache := deviceMetadataCache{
		ClientID:   clientID,
		LocationID: locationID,
		Category:   category,
	}
	jsonData, jsonErr := json.Marshal(metadataToCache)
	if jsonErr != nil {
		// Log the error but still return the data from the fallback.
		// The service can proceed even if caching fails.
		f.logger.Error().Err(jsonErr).Str("device_eui", deviceEUI).Msg("Failed to marshal device data for caching")
		return clientID, locationID, category, nil
	}

	// Set the value in Redis with the configured TTL.
	if setErr := f.redisClient.Set(f.ctx, deviceEUI, jsonData, f.ttl).Err(); setErr != nil {
		f.logger.Error().Err(setErr).Str("device_eui", deviceEUI).Msg("Failed to set device metadata in Redis cache")
	} else {
		f.logger.Debug().Str("device_eui", deviceEUI).Msg("Successfully stored device metadata in Redis cache")
	}

	// Return the data retrieved from the fallback.
	return clientID, locationID, category, nil
}

// Close gracefully closes the Redis client connection.
func (f *RedisDeviceMetadataFetcher) Close() error {
	if f.redisClient != nil {
		f.logger.Info().Msg("Closing Redis client connection...")
		return f.redisClient.Close()
	}
	return nil
}
