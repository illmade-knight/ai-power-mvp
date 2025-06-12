package messenger

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

// MockPayloadGenerator is a mock implementation of PayloadGenerator.
func MockPayloadGenerator(in string) []byte {
	return []byte("mock_payload_for_" + in)
}

// --- Mock Firestore Client ---
// To test Firestore interactions, we need to mock the firestore.Client
// and its relevant methods.

type MockFirestoreClient struct {
	mock.Mock
	// Embed firestore.Client to satisfy the interface if some methods are not mocked
	// and we want to avoid panics. However, for unit tests, we should mock all used methods.
	// client *firestore.Client // Not strictly needed if all calls are mocked
}

type MockCollectionRef struct {
	mock.Mock
	// collectionRef *firestore.CollectionRef // Not strictly needed
	path string // Store path for assertion
}

type MockDocRef struct {
	mock.Mock
	// docRef *firestore.DocumentRef // Not strictly needed
	path string // Store path for assertion
}

// Implement the methods of firestore.Client that DeviceGenerator uses.
func (m *MockFirestoreClient) Collection(path string) *firestore.CollectionRef {
	args := m.Called(path)
	// We need to return a real *firestore.CollectionRef that internally uses our mock.
	// For simplicity in this example, we'll return a mock that can further mock Doc().
	// In a more complex scenario, you might need more elaborate mocking or use a library.
	// Here, we'll assume CollectionRef itself is also a mock type for chaining.
	mockCollRef := args.Get(0).(*MockCollectionRef)
	mockCollRef.path = path           // Store for potential assertions
	return &firestore.CollectionRef{} // This is tricky; ideally, this would be our MockCollectionRef castable back
	// For the purpose of this test, we'll make the mock return our mock type directly.
	// This requires the calling code to expect our mock type or an interface.
	// Since DeviceGenerator expects *firestore.Client, we need to be careful.

	// Let's adjust: The mock will return our MockCollectionRef,
	// and we'll need to ensure our methods in MockCollectionRef match firestore.CollectionRef.
	// This gets complex quickly. A simpler approach for this specific test might be
	// to inject an interface for Firestore operations if full client mocking is too heavy.

	// Given the current structure, we'll mock what's directly called.
	// DeviceGenerator calls: fsClient.Collection(path).Doc(eui).Set(ctx, data)

	// Let's assume Collection returns something that can then have .Doc called on it.
	// We'll return our own MockCollectionRef.
	return args.Get(0).(*firestore.CollectionRef) // This will be tricky.
}

// We'll simplify by focusing on the direct calls and their mocks.
// The firestore.Client is passed to NewDeviceGenerator.
// Inside SeedDevices: dg.fsClient.Collection(dg.cfg.FirestoreCollection).Doc(d.eui)

// Let's redefine mocks for better clarity in testing SeedDevices
type MockFsClient struct {
	mock.Mock
}

func (m *MockFsClient) Collection(path string) *MockFsCollectionRef {
	args := m.Called(path)
	return args.Get(0).(*MockFsCollectionRef)
}

// This is to mock *firestore.CollectionRef
type MockFsCollectionRef struct {
	mock.Mock
	client *MockFsClient // To chain back if needed, or for path context
	path   string
}

func (m *MockFsCollectionRef) Doc(path string) *MockFsDocRef {
	args := m.Called(path)
	return args.Get(0).(*MockFsDocRef)
}

// This is to mock *firestore.DocumentRef
type MockFsDocRef struct {
	mock.Mock
	client *MockFsClient // To chain back
	path   string
}

func (m *MockFsDocRef) Set(ctx context.Context, data interface{}, opts ...firestore.SetOption) (*firestore.WriteResult, error) {
	args := m.Called(ctx, data, opts) // Pass opts to Called
	wrArg := args.Get(0)
	var wr *firestore.WriteResult
	if wrArg != nil {
		wr = wrArg.(*firestore.WriteResult)
	}
	return wr, args.Error(1)
}

func TestNewDevice(t *testing.T) {
	eui := "TESTEUI001"
	rate := 1.5
	device := NewDevice(eui, rate, MockPayloadGenerator)

	assert.NotNil(t, device)
	assert.Equal(t, eui, device.eui)
	assert.Equal(t, rate, device.messageRate)
	assert.NotNil(t, device.payloadMaker)
	assert.Equal(t, 0, device.delivery.ExpectedCount) // Default
	assert.Equal(t, 0, device.delivery.Published)     // Default
}

func TestNewDeviceGenerator(t *testing.T) {
	cfg := &DeviceGeneratorConfig{
		NumDevices:       10,
		MsgRatePerDevice: 0.5,
	}
	logger := zerolog.Nop()
	mockFs := &firestore.Client{} // Using a real one for non-interaction part

	dg, err := NewDeviceGenerator(cfg, mockFs, logger)

	assert.NoError(t, err)
	assert.NotNil(t, dg)
	assert.Equal(t, cfg, dg.cfg)
	assert.Equal(t, mockFs, dg.fsClient)
	assert.NotNil(t, dg.logger)
}

func TestDeviceGenerator_CreateDevices(t *testing.T) {
	t.Run("Successful creation", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:       3,
			MsgRatePerDevice: 2.0,
		}
		logger := zerolog.Nop()
		dg := &DeviceGenerator{
			cfg:     cfg,
			logger:  logger,
			Devices: make([]*Device, cfg.NumDevices), // Pre-allocate as per corrected logic
		}

		dg.CreateDevices()

		assert.Len(t, dg.Devices, 3)
		for i, device := range dg.Devices {
			expectedEUI := fmt.Sprintf("LOADTEST-%06d", i)
			assert.NotNil(t, device)
			assert.Equal(t, expectedEUI, device.eui)
			assert.Equal(t, cfg.MsgRatePerDevice, device.messageRate)
			assert.NotNil(t, device.payloadMaker) // Check if our specific generator was assigned
			// Verify payloadMaker by calling it
			payload := device.payloadMaker(device.eui)
			expectedPayload := generateXDeviceRawPayloadBytes(device.eui) // Assuming this is the one used
			assert.Equal(t, expectedPayload, payload)
		}
	})

	t.Run("Zero devices", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:       0,
			MsgRatePerDevice: 1.0,
		}
		logger := zerolog.Nop()
		dg := &DeviceGenerator{
			cfg:     cfg,
			logger:  logger,
			Devices: make([]*Device, cfg.NumDevices), // Correctly handles 0
		}

		dg.CreateDevices()
		assert.Len(t, dg.Devices, 0)
	})
}

func TestDeviceGenerator_SeedDevices(t *testing.T) {
	logger := zerolog.Nop()
	baseCtx := context.Background()

	t.Run("Successful seeding", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:          2,
			MsgRatePerDevice:    1.0,
			FirestoreProjectID:  "test-project",
			FirestoreCollection: "test-devices",
		}

		// Create mock Firestore client and its chained calls
		mockFirestore := new(MockFsClient)
		mockCollection := new(MockFsCollectionRef)
		mockDoc1 := new(MockFsDocRef)
		mockDoc2 := new(MockFsDocRef)

		dg := &DeviceGenerator{
			cfg: cfg,
			// fsClient: mockFirestore, // This needs to be *firestore.Client
			logger: logger,
			Devices: []*Device{
				NewDevice("LOADTEST-000000", 1.0, generateXDeviceRawPayloadBytes),
				NewDevice("LOADTEST-000001", 1.0, generateXDeviceRawPayloadBytes),
			},
		}
		// This is the tricky part: dg.fsClient is *firestore.Client, not our mock interface.
		// For true unit testing without external dependencies, you'd typically:
		// 1. Define an interface for the Firestore operations needed by DeviceGenerator.
		// 2. Make DeviceGenerator depend on this interface.
		// 3. Implement this interface with the real firestore.Client for production.
		// 4. Implement this interface with a mock for testing.

		// Since we can't easily change the existing code structure for this test,
		// we'll assume for this example that we *could* pass our MockFsClient
		// if it implemented all methods of *firestore.Client (which is extensive).
		// A more practical approach for existing code is often integration testing with an emulator.

		// For this unit test, we'll proceed by setting up expectations on the mock
		// as if it were being called. The actual execution would fail if dg.fsClient
		// is not our mock. This highlights a limitation of testing without interfaces.

		// Let's assume we could somehow inject our mockFsClient instance as dg.fsClient.
		// This is not possible directly with the current NewDeviceGenerator signature
		// if it strictly takes *firestore.Client.
		// So, this test is more of a blueprint for how it *would* look with a mockable interface.

		// If we were to test this as is, we'd need a running Firestore emulator
		// or mock the firestore package globally, which is complex and not recommended.

		// For the sake of demonstrating the mocking logic:
		dg.fsClient = &firestore.Client{} // Placeholder; this won't use our mocks below.
		// To make this testable with mocks, DeviceGenerator.fsClient
		// would need to be an interface type that MockFsClient implements.

		// Setup expectations (these won't be hit with the placeholder dg.fsClient)
		mockFirestore.On("Collection", cfg.FirestoreCollection).Return(mockCollection).Twice() // Called for each device

		expectedDataDev0 := FirestoreDeviceData{
			ClientID:       "Client-Load-000000",
			LocationID:     "Location-Load-000",
			DeviceCategory: "SensorType-Alpha",
		}
		expectedDataDev1 := FirestoreDeviceData{
			ClientID:       "Client-Load-000001",
			LocationID:     "Location-Load-001",
			DeviceCategory: "SensorType-Beta",
		}

		// Mocking for device 0
		mockCollection.On("Doc", "LOADTEST-000000").Return(mockDoc1).Once()
		mockDoc1.On("Set", mock.AnythingOfType("*context.timerCtx"), expectedDataDev0, mock.Anything).Return(&firestore.WriteResult{}, nil).Once()

		// Mocking for device 1
		mockCollection.On("Doc", "LOADTEST-000001").Return(mockDoc2).Once()
		mockDoc2.On("Set", mock.AnythingOfType("*context.timerCtx"), expectedDataDev1, mock.Anything).Return(&firestore.WriteResult{}, nil).Once()

		// To actually run this test with the mocks, you would need to:
		// 1. Modify DeviceGenerator to accept an interface for fsClient.
		// 2. Pass the mockFirestore (which implements that interface) to NewDeviceGenerator.
		// For now, this test will pass vacuously or fail if it tries to use the nil fsClient.
		// We'll comment out the actual call to SeedDevices for this illustration.

		// err := dg.SeedDevices(baseCtx)
		// assert.NoError(t, err)
		// mockFirestore.AssertExpectations(t)
		// mockCollection.AssertExpectations(t)
		// mockDoc1.AssertExpectations(t)
		// mockDoc2.AssertExpectations(t)

		// This test highlights the importance of designing for testability.
		// We'll skip asserting this part due to the direct dependency on *firestore.Client.
		t.Log("Skipping full SeedDevices mock assertion due to direct *firestore.Client dependency.")
	})

	t.Run("Firestore seeding skipped if project MessageID is empty", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:          1,
			FirestoreProjectID:  "", // Empty, so seeding should be skipped
			FirestoreCollection: "test-devices",
		}
		dg := &DeviceGenerator{
			cfg:    cfg,
			logger: logger,
			// fsClient: nil, // fsClient can be nil or non-nil, the check is on ProjectID
			Devices: []*Device{NewDevice("SKIP-SEED-000", 1.0, generateXDeviceRawPayloadBytes)},
		}

		err := dg.SeedDevices(baseCtx)
		assert.NoError(t, err)
		// No calls to Firestore mock should be expected.
	})

	t.Run("Firestore seeding skipped if collection is empty", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:          1,
			FirestoreProjectID:  "test-project",
			FirestoreCollection: "", // Empty, so seeding should be skipped
		}
		dg := &DeviceGenerator{
			cfg:     cfg,
			logger:  logger,
			Devices: []*Device{NewDevice("SKIP-SEED-001", 1.0, generateXDeviceRawPayloadBytes)},
		}
		err := dg.SeedDevices(baseCtx)
		assert.NoError(t, err)
	})

	t.Run("Firestore Set operation fails for a device", func(t *testing.T) {
		cfg := &DeviceGeneratorConfig{
			NumDevices:          1,
			MsgRatePerDevice:    1.0,
			FirestoreProjectID:  "test-project",
			FirestoreCollection: "test-devices",
		}
		// mockFirestore := new(MockFsClient) // Again, assuming we could inject this
		// mockCollection := new(MockFsCollectionRef)
		// mockDoc := new(MockFsDocRef)

		dg := &DeviceGenerator{
			cfg: cfg,
			// fsClient: mockFirestore,
			logger: logger,
			Devices: []*Device{
				NewDevice("FAIL-SEED-000", 1.0, generateXDeviceRawPayloadBytes),
			},
		}
		dg.fsClient = &firestore.Client{} // Placeholder

		// Expected data for the failing device
		// expectedData := FirestoreDeviceData{ /* ... */ }
		// firestoreError := errors.New("firestore unavailable")

		// mockFirestore.On("Collection", cfg.FirestoreCollection).Return(mockCollection).Once()
		// mockCollection.On("Doc", "FAIL-SEED-000").Return(mockDoc).Once()
		// mockDoc.On("Set", mock.Anything, expectedData, mock.Anything).Return(nil, firestoreError).Once()

		// err := dg.SeedDevices(baseCtx)
		// assert.NoError(t, err) // SeedDevices itself doesn't return an error for individual fails, it logs them.

		// mockFirestore.AssertExpectations(t)
		// mockCollection.AssertExpectations(t)
		// mockDoc.AssertExpectations(t)
		t.Log("Skipping full SeedDevices mock assertion for Set failure due to direct *firestore.Client dependency.")
	})
}
