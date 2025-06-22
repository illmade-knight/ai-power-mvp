## Icestore with GCP resources

### gemini notes

we asked the agent to create unique bucket names 

````go
package main

    sanitizedTestName := strings.ReplaceAll(tc.name, "_", "-")
	sanitizedTestName = strings.ReplaceAll(sanitizedTestName, " ", "-")
	sanitizedTestName = strings.ToLower(sanitizedTestName)
	
	// Determine max length for the sanitized test name part
	// cloudGCSBucketPrefix (e.g., "e2e-garden-monitor-test") + first hyphen + timestamp (e.g., "20060102150405") + second hyphen = 22 + 1 + 14 + 1 = 38 chars
	// Remaining for test name suffix = 63 - 38 = 25 chars
	maxTestNameSuffixLength := maxBucketNameLength - len(cloudGCSBucketPrefix) - 1 - 14 - 1 // 1 for first hyphen, 14 for YYYYMMDDHHMMSS, 1 for second hyphen
	
	if len(sanitizedTestName) > maxTestNameSuffixLength {
		sanitizedTestName = sanitizedTestName[:maxTestNameSuffixLength]
	}
	// IMPORTANT FIX: Ensure the sanitizedTestName does not end with a hyphen after truncation
	sanitizedTestName = strings.TrimSuffix(sanitizedTestName, "-")
	
	// Generate a unique bucket name for this test run
	// Using a more compact timestamp format: YYYYMMDDHHMMSS (14 chars)
    uniqueBucketName := fmt.Sprintf("%s%s-%s", cloudGCSBucketPrefix, time.Now().Format("20060102150405"), sanitizedTestName)

````

me: 

that code seems a bit of a mess to me - can't we use a uuid generator or something

agent response:

You're right, the current bucket naming logic can be simplified and made more robust by using a UUID. It will make the code cleaner and ensure uniqueness without complex length calculations or manual sanitization for the identifier part.

I've updated the "End-to-End Cloud Test for MQTT to IceStore Flow (Final Bucket Naming Fix)" Canvas to incorporate a UUID for generating unique bucket names. This replaces the timestamp and truncated test name logic with a simpler and more reliable approach.
