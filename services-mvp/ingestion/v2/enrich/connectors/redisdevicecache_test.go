package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-redis/redismock/v8"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockFallbackFetcher is a controllable mock for the DeviceMetadataFetcher.
// It allows us to simulate different outcomes from the fallback (e.g., success, not found).
type mockFallbackFetcher struct {
	// Function to execute when called. This allows for dynamic behavior per test.
	fetchFunc func(deviceEUI string) (string, string, string, error)
	// Tracks how many times the fallback was invoked.
	callCount int
}

// Fetch executes the mock's fetchFunc and increments the call count.
func (m *mockFallbackFetcher) Fetch(deviceEUI string) (string, string, string, error) {
	m.callCount++
	return m.fetchFunc(deviceEUI)
}

// TestRedisDeviceMetadataFetcher covers the core scenarios for the caching fetcher.
func TestRedisDeviceMetadataFetcher(t *testing.T) {
	// --- Test Setup ---
	ctx := context.Background()
	// Mute logger for tests to keep test output clean.
	logger := zerolog.New(io.Discard)
	ttl := 5 * time.Minute

	knownEUI := "KNOWN-EUI-01"
	knownMetadata := deviceMetadataCache{
		ClientID:   "test-client",
		LocationID: "test-location",
		Category:   "test-category",
	}
	knownMetadataJSON, err := json.Marshal(knownMetadata)
	require.NoError(t, err)

	unknownEUI := "UNKNOWN-EUI-01"

	// --- Test Cases ---

	t.Run("Cache Hit", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		fallback := &mockFallbackFetcher{} // Should not be called
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a GET call to Redis for the known EUI, and return the metadata.
		mock.ExpectGet(knownEUI).SetVal(string(knownMetadataJSON))

		// Act
		clientID, locationID, category, err := fetcher.Fetch(knownEUI)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, knownMetadata.ClientID, clientID)
		assert.Equal(t, knownMetadata.LocationID, locationID)
		assert.Equal(t, knownMetadata.Category, category)

		// Crucially, the fallback should NOT have been called.
		assert.Equal(t, 0, fallback.callCount, "Fallback fetcher should not be called on cache hit")
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})

	t.Run("Cache Miss and Successful Fallback", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		fallback := &mockFallbackFetcher{
			fetchFunc: func(eui string) (string, string, string, error) {
				assert.Equal(t, knownEUI, eui)
				return knownMetadata.ClientID, knownMetadata.LocationID, knownMetadata.Category, nil
			},
		}
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a GET call (which will miss) and then a SET call to cache the new data.
		mock.ExpectGet(knownEUI).RedisNil()
		mock.ExpectSet(knownEUI, knownMetadataJSON, ttl).SetVal("OK")

		// Act
		clientID, locationID, category, err := fetcher.Fetch(knownEUI)

		// Assert
		require.NoError(t, err)
		assert.Equal(t, knownMetadata.ClientID, clientID)
		assert.Equal(t, knownMetadata.LocationID, locationID)
		assert.Equal(t, knownMetadata.Category, category)

		// The fallback should have been called exactly once.
		assert.Equal(t, 1, fallback.callCount, "Fallback fetcher should be called on cache miss")
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})

	t.Run("Corrupted Cache Data leads to Fallback", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		fallback := &mockFallbackFetcher{
			fetchFunc: func(eui string) (string, string, string, error) {
				return knownMetadata.ClientID, knownMetadata.LocationID, knownMetadata.Category, nil
			},
		}
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a GET call which returns corrupted data, followed by a SET to fix it.
		mock.ExpectGet(knownEUI).SetVal("this is not valid json")
		mock.ExpectSet(knownEUI, knownMetadataJSON, ttl).SetVal("OK")

		// Act
		clientID, _, _, err := fetcher.Fetch(knownEUI)

		// Assert
		require.NoError(t, err, "Fetch should succeed by using the fallback")
		assert.Equal(t, knownMetadata.ClientID, clientID)
		assert.Equal(t, 1, fallback.callCount, "Fallback should be called when cache data is corrupt")
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})

	t.Run("Cache Miss and Fallback Error", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		fallbackError := ErrMetadataNotFound // Use a defined error
		fallback := &mockFallbackFetcher{
			fetchFunc: func(eui string) (string, string, string, error) {
				return "", "", "", fallbackError
			},
		}
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a GET call, which will miss. No SET call should happen because the fallback failed.
		mock.ExpectGet(unknownEUI).RedisNil()

		// Act
		_, _, _, err := fetcher.Fetch(unknownEUI)

		// Assert
		require.Error(t, err)
		assert.True(t, errors.Is(err, fallbackError), "Error should be the one from the fallback")

		// The fallback was called, but caching was skipped due to the error.
		assert.Equal(t, 1, fallback.callCount, "Fallback fetcher should still be called")
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})

	t.Run("Redis GET Fails should still succeed via Fallback", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		redisError := errors.New("network connection lost")
		fallback := &mockFallbackFetcher{
			fetchFunc: func(eui string) (string, string, string, error) {
				// This should still be called as a fallback mechanism.
				return knownMetadata.ClientID, knownMetadata.LocationID, knownMetadata.Category, nil
			},
		}
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a GET to fail, but the SET should still be attempted after a successful fallback.
		mock.ExpectGet(knownEUI).SetErr(redisError)
		mock.ExpectSet(knownEUI, knownMetadataJSON, ttl).SetVal("OK")

		// Act
		clientID, _, _, err := fetcher.Fetch(knownEUI)

		// Assert
		// The function should succeed overall by returning data from the fallback. The Redis error is logged but swallowed.
		require.NoError(t, err)
		assert.Equal(t, knownMetadata.ClientID, clientID)

		// The fallback must be called because Redis failed.
		assert.Equal(t, 1, fallback.callCount, "Fallback must be called when Redis GET fails")
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})

	t.Run("Redis SET Fails After Cache Miss", func(t *testing.T) {
		// Arrange
		db, mock := redismock.NewClientMock()
		redisSetError := errors.New("could not write to cache")
		fallback := &mockFallbackFetcher{
			fetchFunc: func(eui string) (string, string, string, error) {
				return knownMetadata.ClientID, knownMetadata.LocationID, knownMetadata.Category, nil
			},
		}
		fetcher := &RedisDeviceMetadataFetcher{
			redisClient:     db,
			fallbackFetcher: fallback.Fetch,
			logger:          logger,
			ttl:             ttl,
			ctx:             ctx,
		}

		// Expect a miss on GET, then a failure on SET.
		mock.ExpectGet(knownEUI).RedisNil()
		mock.ExpectSet(knownEUI, knownMetadataJSON, ttl).SetErr(redisSetError)

		// Act
		clientID, _, _, err := fetcher.Fetch(knownEUI)

		// Assert
		// The operation should still succeed overall, returning data from the fallback. The SET failure is only logged.
		require.NoError(t, err)
		assert.Equal(t, knownMetadata.ClientID, clientID)

		// The fallback should have been called once.
		assert.Equal(t, 1, fallback.callCount)
		assert.NoError(t, mock.ExpectationsWereMet(), "Redis mock expectations not met")
	})
}
